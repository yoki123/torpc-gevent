# -*- coding: utf-8 -*-

import logging

import gevent

from gtorpc import RPCServer, RPClient

if __name__ == '__main__':
    log_format = '%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger(__name__)

    address = ('127.0.0.1', 5000)


    def test_server():
        server = RPCServer(address)

        @server.service.route()
        def sum(x, y):
            return x + y

        @server.service.route()
        def ping_node1():
            return server.call_node('node1', 'ping')

        server.start()


    def test_node1():
        c = RPClient(address)

        @c.service.route()
        def ping():
            return 'pong from node1'

        logger.info('node1->register: %s' % c.register('node1'))


    def test_node2():
        c = RPClient(address)

        @c.service.route()
        def ping():
            return 'pong'

        logger.info("node2->register: %s " % c.register('node2'))
        logger.info("node2->sum: %s " % c.call('sum', 11, 22))
        logger.info("node2->call_node: %s " % c.call('call_node', "node1", "ping"))
        logger.info("node2->ping_node1: %s " % c.call('ping_node1'))


    gevent.spawn(test_server)
    gevent.spawn(test_node1)
    gevent.spawn(test_node2)
    gevent.wait()
