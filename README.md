# torpc-gevent
一个基于Gevent的双向RPC的实现，通信接口兼容[ToRPC](https://github.com/yoki123/torpc)。


## 样例
### RPC服务器
```python
server = RPCServer(address)

@server.service.route()
def sum(x, y):
    return x + y

@server.service.route()
def ping_node1():
    return server.call_node('node1', 'ping')

server.start()
```

### RPC客户端1
```python
def test_node1():
    c = RPClient(address)

    @c.service.route()
    def ping():
        return 'pong from node1'

    print('node1->register: %s' % c.register('node1'))
```

### RPC客户端2
```python
def test_node2():
    c = RPClient(address)

    @c.service.route()
    def ping():
        return 'pong'
    print("node2->register: %s " % c.register('node2'))
    print("node2->sum: %s " % c.call('sum', 11, 22))
    print("node2->call_node: %s " % c.call('call_node', "node1", "ping"))
    print("node2->ping_node1: %s " % c.call('ping_node1'))
```

### 同时运行
```python
# 这里是单进程中同时运行
gevent.spawn(test_server)
gevent.spawn(test_node1)
gevent.spawn(test_node2)
gevent.wait()
```

### 运行结果
```
node1->register: True
node2->register: True 
node2->sum: 33 
node2->call_node: pong from node1 
node2->ping_node1: pong from node1
```