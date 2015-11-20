#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import unittest
import errno


class RunningException(Exception):

    def __init__(self, msg, item=None):
        if item is not None:
            msg = '%s: %r' % (msg, item)
        super(RunningException, self).__init__(msg)


class Client(object):

    def __init__(self, host, port, timeout=None):
        self._addr = (host, port)
        self._timeout = timeout
        self._socket = None

    def close(self):
        if self._socket:
            self._socket.close()
            self._socket = None

    def __del__(self):
        self.close()

    def _connect(self):
        # 若重连清空buffer
        self._buffer = ''

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self._timeout) # 设置超时时间
        try:
            self._socket.connect(self._addr)
            self._socket.settimeout(self._timeout)
        except (socket.error, socket.timeout):
            self._socket = None # 设置为None, 方便close
            raise

    def _read(self, length=None):
        '''
        从服务返回 length 字节的数据
        如果长度为0,返回一个被"/r/n'分割的回复
        '''
        result = None
        while result is None:
            if length:
                if len(self._buffer) >= length:
                    result = self._buffer[:length]
                    self._buffer = self._buffer[length:]
            else:
                delim_index = self._buffer.find('\r\n')
                if delim_index != -1:
                    result = self._buffer[:delim_index+2]
                    self._buffer = self._buffer[delim_index+2:]

            if result is None: # 没获取到数据,说明服务器数据没有获取完全
                try:
                    tmp = self._socket.recv(4096)
                except (socket.error, socket.timeout) as e:
                    self.close()
                    raise e

                if not tmp:
                    raise socket.error, 'unexpected socket close on recv'
                else:
                    self._buffer += tmp
        return result

    def _send_command(self, command):
        if self._socket: # 若连接还在
            try:
                self._socket.settimeout(0)
                self._socket.recv(0)
                # 通过设置timeout来引发except,如果没有引发异常,则说明连接已经关闭或者有垃圾数据在缓冲区中
                self.close()
            except socket.error as e:
                if e.errno == errno.EAGAIN: # 这是期望的异常,重新设置timeout
                    self._socket.settimeout(self._timeout)
                else:
                    self.close()

        if not self._socket: # 若断开则重练
            self._connect()

        self._socket.sendall(command)
        return self._read()

    def _validate_key(self, key):
        if not isinstance(key, str):
            raise RunningException('key must be string', key)
        return key

    def multi_get(self, keys):
        if len(keys) == 0:
            return []

        # 请求  - get <key> [<key> ...]\r\n
        # 回复 - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        #        <data block>\r\n (if exists)
        #        [...]
        #        END\r\n
        keys = [self._validate_key(key) for key in keys]
        command = 'get %s\r\n' % ' '.join(keys)
        received = {}
        resp = self._send_command(command)
        error = None

        while resp != 'END\r\n':
            terms = resp.split()
            if len(terms) == 4 and terms[0] == 'VALUE': # key存在
                key = terms[1]
                flags = int(terms[2])
                length = int(terms[3])
                val = self._read(length+2)[:-2] # 读到"/r/n",让下次读取正常
                received[key] = val
            else: # key不存在
                raise RunningException('get failed', resp)
            resp = self._read()


        if len(received) > len(keys):
            raise RunningException('received too many responses')

        if len(keys) == 1 and len(received) == 1:
            response = received.values()
        else:
            response = [received.get(key) for key in keys]
        return response

    def get(self, *keys):
        return self.multi_get(keys)[0]

    def set(self, key, val, exptime=0):
        '''
        设置key val以及失效时间exptime 失效时间0代表不要自动失效
        '''
        # 请求  - set <key> <flags> <exptime> <bytes> [noreply]\r\n
        #        <data block>\r\n
        # 回复 - STORED\r\n (or others)
        key = self._validate_key(key)


        if not isinstance(exptime, int):
            raise RunningException('exptime not int', exptime)
        elif exptime < 0:
            raise RunningException('exptime negative', exptime)

        command = 'set %s 0 %d %d\r\n%s\r\n' % (key, exptime, len(val), val)
        resp = self._send_command(command)
        if resp != 'STORED\r\n':
            raise RunningException('set failed', resp)




