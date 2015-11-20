#!/usr/bin/env python
# -*- coding: utf-8 -*-


from client import Client
import socket
import time
import unittest


class TestClient(unittest.TestCase):

    def setUp(self):
        self.client = Client('127.0.0.1', 11211)

    def tearDown(self):
        self.client.close()

    def test_multi_get(self):
        items = {'hehe':'haha', 'dd':'aa', 'cc':'ee'}
        for k, v in items.items():
            self.client.set(k, v)
        resp = self.client.multi_get(items.keys())
        for v, r in zip(items.values(), resp):
            self.assertTrue(v == r)

    def test_expire(self):
        key = 'expire'
        val = "time is elapsed"
        self.client.set(key, val, exptime=1)
        time.sleep(2)
        mcval = self.client.get(key)
        self.assertEqual(mcval, None)


    def test_get_unknown(self):
        mcval = self.client.get('get_unknown')
        self.assertEqual(mcval, None)

    def test_set_bad(self):
        key = 'set_bad'
        self.assertRaises(Exception, self.client.set, key, '!' * 1024**2)
        #self.client.set(key, '!' * (1024**2 - 100))
        self.assertRaises(Exception, self.client.set, '', 'empty key')

    def test_set_get(self):
        key = 'set_get'
        val = "eJsiIU"
        self.client.set(key, val)
        mcval = self.client.get(key)
        self.assertEqual(mcval, val)

class TestConnectTimeout(unittest.TestCase):

    unavailable_ip = '4.4.4.4'

    def test_connect_timeout(self):
        client = Client(self.unavailable_ip, 11211, timeout=1)
        self.assertRaises(socket.timeout, client._connect)
        client.close()


if __name__ == '__main__':

    suite = unittest.TestSuite([
        unittest.TestLoader().loadTestsFromTestCase(TestClient),
        unittest.TestLoader().loadTestsFromTestCase(TestConnectTimeout),
    ])

    unittest.TextTestRunner(verbosity=2).run(suite)