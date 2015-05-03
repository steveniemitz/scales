from collections import deque
from struct import unpack
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult
from gevent.queue import Queue

import hexdump

from types import MessageType
from message import (
  Deadline,
  DispatchMessage,
  RdispatchMessage,
  PingMessage)

class ServerException(Exception):  pass

class TagPool(object):
  def __init__(self):
    self._queue = deque()
    self._next = 1

  def get(self):
    if not any(self._queue):
      self._next += 1
      return self._next
    else:
      return self._queue.pop()

  def release(self, tag):
    self._queue.append(tag)


class MessageDispatcher(object):
  def __init__(self, socket):
    self._socket = socket
    self._socket.open()
    self._tag_pool = TagPool()
    self._send_queue = Queue()
    self._tag_map = {}
    self._handlers = {
      MessageType.Rdispatch: self._HandleRdispatch,
      MessageType.Rping: self._HandleRping,
      }
    gevent.spawn(self._SendLoop)
    gevent.spawn(self._RecvLoop)
    gevent.spawn(self._PingLoop)

  def _SendLoop(self):
    while True:
      payload = self._send_queue.get()
      print hexdump.dump(payload)
      self._socket.write(payload)

  def _RecvLoop(self):
    while True:
      sz, = unpack('!i', self._socket.readAll(4))
      buf = StringIO(self._socket.readAll(sz))
      gevent.spawn(self._ReadMessage, buf)

  def _PingLoop(self):
    while True:
      gevent.sleep(30)
      self._SendPingMessage()

  def _HandleRdispatch(self, msg):
    msg_cls = RdispatchMessage.Unmarshal(msg)
    return msg_cls

  @staticmethod
  def _HandleRping(msg):
    return None

  @staticmethod
  def _ReadHeader(msg):
    header, = unpack('!i', msg.read(4))
    msg_type = (256 - (header >> 24 & 0xff)) * -1
    tag = ((header << 8) & 0xFFFFFFFF) >> 8
    return msg_type, tag

  def _ReadMessage(self, msg):
    msg_type, tag = self._ReadHeader(msg)
    try:
      msg_cls = self._handlers[msg_type](msg)
    finally:
      ar = self._tag_map.pop(tag, None)
      self._tag_pool.release(tag)
    if ar:
      if msg_cls.err:
        ar.set_exception(ServerException(msg_cls.err))
      else:
        ar.set(msg_cls.buf)

  def SendDispatchMessage(self, thrift_payload, timeout=None):
    ctx = {}
    if timeout:
      ctx['com.twitter.finagle.Deadline'] = Deadline(timeout)

    disp_msg = DispatchMessage(thrift_payload, ctx)
    return self._SendMessage(disp_msg)

  def _SendPingMessage(self):
    ping_msg = PingMessage()
    return self._SendMessage(ping_msg)

  def _SendMessage(self, msg):
    msg.tag = self._tag_pool.get()

    fpb = StringIO()
    msg.Marshal(fpb)
    finagle_payload = fpb.getvalue()

    ar = AsyncResult()
    self._tag_map[msg.tag] = ar
    self._send_queue.put(finagle_payload)
    return ar
