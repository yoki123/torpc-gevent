# -*- coding: utf-8 -*-

import struct
import logging
import traceback

import gevent
import msgpack as packer
from gevent.event import AsyncResult
from gevent.greenlet import Greenlet
from gevent.server import StreamServer
from gevent.socket import create_connection

LOG = logging.getLogger(__name__)

RPC_REQUEST = 0
RPC_RESPONSE = 1
RPC_NOTICE = 2
RPC_REGISTER = 3  # 用于RPC注册
HEAD_LEN = struct.calcsize('!ibi')  # RPC包头长度，其中!ibi表示data_length, msg_type, msg_id
MAX_DATA_LEN = 4096  # 最大数据长度（不含包头），可自定义


def _id_generator():
    """id生成器"""
    counter = 0
    while True:
        yield counter
        counter += 1
        if counter > (1 << 30):
            counter = 0


class RPCServerError(Exception):
    """RPC服务器错误"""
    pass


class RPCRegisterError(Exception):
    """RPC注册错误"""
    pass


class RPCTimeOutError(Exception):
    """RPC超时错误"""
    pass


class NoServiceError(Exception):
    """无服务错误"""
    pass


class Services(object):
    def __init__(self):
        self._targets = {}

    def dispatch(self, key, func):
        """注册服务"""
        if key in self._targets:
            raise KeyError(key)
        self._targets[key] = func

    def route(self, specific_key=0):
        """装饰方式 注册服务"""

        def decorator(func):
            if specific_key:
                _key = specific_key
            else:
                _key = func.__name__
            if _key in self._targets:
                raise KeyError(_key)
            self._targets[_key] = func
            return func

        return decorator

    def call(self, command, *args):
        """调用服务"""
        method = self._targets.get(command)
        if not method:
            raise NoServiceError(command)
        # LOG.debug('call method %s' % method.__name__)
        return method(*args)


class RPCConnection(Greenlet):
    """RPC连接协程"""

    def __init__(self, sock, service=None, close_callback=None):
        self._buff = ''
        self.sock = sock
        self._timeout = 10  # 请求超时时间
        self._id_iter = _id_generator()  # 消息id生成器
        self._request_table = {}  # 保存所有的RPC请求的AsyncResult，key对应包ID，范围为30bit，在timeout时间内，理论上不可能重复
        self.service = service
        self.close_callback = close_callback  # 断开毁掉
        Greenlet.__init__(self)

    def _run(self):
        """主循环"""
        while True:
            try:
                data = self.sock.recv(1024)
            except Exception as e:
                LOG.debug(str(e))
                break
            if not data:
                break
            self.on_receive(data)
        self.close_callback(self)

    def write(self, data):
        self.sock.sendall(data)

    def _pack_request(self, msg_id, msg_type, method_name, arg):
        """打包请求"""
        buf = packer.dumps((method_name, arg))
        return struct.pack('!ibi', len(buf), msg_type, msg_id) + buf

    def _pack_response(self, msg_id, msg_type, err, result):
        """打包响应"""
        buf = packer.dumps((err, result))
        return struct.pack('!ibi', len(buf), msg_type, msg_id) + buf

    def on_receive(self, data):
        """消息处理"""
        # 粘包处理
        self._buff += data
        _cur_len = len(self._buff)

        while _cur_len >= HEAD_LEN:
            # 多次接受的包都不能组成完整的包时，unpack会调用多次, should fix?
            body_length, msg_type, msg_id = struct.unpack('!ibi', self._buff[:HEAD_LEN])

            if body_length > MAX_DATA_LEN:
                LOG.debug('data too long!')
                return self.sock.close()

            if _cur_len - HEAD_LEN >= body_length:
                request = self._buff[HEAD_LEN:HEAD_LEN + body_length]

                # 拆包处理
                self._buff = self._buff[HEAD_LEN + body_length:]

                _cur_len = len(self._buff)

                # 解析body
                try:
                    req = packer.loads(request)
                except Exception as e:
                    LOG.debug('packer loads error->%s' % str(e))
                    return

                if msg_type == RPC_REQUEST:
                    (method_name, args) = req

                    # 测试，使用gevent.pool效率比gevent.spawn更低！？
                    gevent.spawn(self.handle_request, msg_id, method_name, *args)
                    # self.handle_request(msg_id, method_name, *args)

                elif msg_type == RPC_RESPONSE:
                    (err, response) = req
                    self.handle_response(msg_id, err, response)

                elif msg_type == RPC_NOTICE:
                    (method_name, args) = req
                    gevent.spawn(self.handle_notice, msg_id, method_name, *args)
                    # self.handle_notice(msg_id, method_name, args)

                elif msg_type == RPC_REGISTER:
                    (method_name, args) = req
                    self.handle_register(msg_id, method_name, args)
            else:
                break

    def handle_request(self, msg_id, method_name, *args):
        """处理RPC请求"""
        err = None
        result = None
        try:
            result = self.service.call(method_name, *args)
        except Exception:
            err = str(traceback.format_exc())
        finally:
            buf = self._pack_response(msg_id, RPC_RESPONSE, err, result)
            self.write(buf)

    def handle_notice(self, msg_id, method_name, args):
        """处理RPC通知"""
        try:
            self.service.call(method_name, *args)
        except Exception as e:
            LOG.error("call %s error in handle_rpc_notice:%s" % (method_name, str(e)))

    def handle_register(self, msg_id, method_name, *args):
        """处理RPC注册"""
        err = None
        result = None
        try:
            result = self.service.call(method_name, self, *args)
        except Exception:
            err = str(traceback.format_exc())
        finally:
            buf = self._pack_response(msg_id, RPC_RESPONSE, err, result)
            self.write(buf)

    def handle_response(self, msg_id, err, ret):
        """处理RPC回答"""
        if msg_id not in self._request_table:
            LOG.debug('response time out?')
            return
        if err:
            raise RPCServerError(err)

        _async_result = self._request_table.pop(msg_id)
        _async_result.set(ret)

    def call(self, method_name, *arg):
        """调用RPC，得到结果前会阻塞协程，超时会抛异常Timeout """
        msg_id = next(self._id_iter)
        buff = self._pack_request(msg_id, RPC_REQUEST, method_name, arg)
        _async_result = AsyncResult()
        self._request_table[msg_id] = _async_result
        self.write(buff)
        return _async_result.get(timeout=self._timeout)

    def notice(self, method_name, *arg):
        msg_id = next(self._id_iter)
        buf = self._pack_request(msg_id, RPC_NOTICE, method_name, arg)
        self.write(buf)

    def register(self, *arg):
        """RPC注册，得到结果前会阻塞协程。超时会抛异常 Timeout"""
        msg_id = next(self._id_iter)
        buf = self._pack_request(msg_id, RPC_REGISTER, 'register', *arg)
        _async_result = AsyncResult()
        self._request_table[msg_id] = _async_result
        self.write(buf)
        return _async_result.get(timeout=self._timeout)


class RPCServer(StreamServer):
    def __init__(self, listener, **server_args):
        self.nodes = {}  # 所有RPC客户端节点
        self.service = Services()
        self.service.dispatch('call_node', self.call_node)
        self.service.dispatch('register', self._handle_register)
        StreamServer.__init__(self, listener, spawn=None, **server_args)

    def handle(self, conn, address):
        RPCConnection.spawn(conn, self.service, self.on_closed)

    def on_closed(self, conn):
        """RPC连接断开处理"""
        _node_name = None
        for _name, _conn in self.nodes.iteritems():
            if _conn == conn:
                _node_name = _name
                break
        if _node_name:
            self.nodes.pop(_node_name)
            LOG.debug('%s disconnect' % _node_name)

    def call_node(self, name, method_name, *arg):
        """调用RPC已注册的节点，也是RPC服务器的一个可以被调用的方法"""
        if name not in self.nodes:
            raise Exception('node {0} not exist'.format(name))
        node = self.nodes[name]
        return node.call(method_name, *arg)

    def _handle_register(self, conn, name):
        if name in self.nodes:
            LOG.warning('[%s] already register' % name)
            return False
        self.nodes[name] = conn
        LOG.debug('[%s] register success' % name)
        return True


class RPClient(object):
    """RPC客户端"""

    def __init__(self, address):
        self.service = Services()
        sock = create_connection(address)
        conn = RPCConnection.spawn(sock, self.service, self.on_closed)
        self.conn = conn
        self._closed = False

    def start(self):
        pass

    def call(self, method, *args):
        return self.conn.call(method, *args)

    def register(self, name):
        return self.conn.register(name)

    def notice(self, method, *args):
        return self.conn.notice(method, *args)

    def on_closed(self, _):
        self._closed = True
