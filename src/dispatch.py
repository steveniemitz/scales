from struct import unpack
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult
from gevent.queue import Queue

from types import (
  DispatcherState,
  MessageType)
from message import (
  Deadline,
  DiscardedMessage,
  DispatchMessage,
  RMessage,
  RdispatchMessage,
  PingMessage)

class ServerException(Exception):  pass
class DispatcherException(Exception): pass
class TimeoutException(Exception): pass

class TagPool(object):
  def __init__(self):
    self._set = set()
    self._next = 1

  def get(self):
    if not any(self._set):
      self._next += 1
      return self._next
    else:
      return self._set.pop()

  def release(self, tag):
    self._set.add(tag)


class MessageDispatcher(object):
  class Handlers(object):
    @staticmethod
    def Rdispatch(msg):
      msg_cls = RdispatchMessage.Unmarshal(msg)
      return msg_cls

    @staticmethod
    def Rping(msg):
      return RMessage(MessageType.Rping)

  def __init__(self, socket):
    self._socket = socket
    self._socket.open()
    self._tag_pool = TagPool()
    self._send_queue = Queue()
    self._tag_map = {}
    self._state = DispatcherState.RUNNING
    self._exception = None
    self._handlers = {
      MessageType.Rdispatch: self.Handlers.Rdispatch,
      MessageType.Rping: self.Handlers.Rping,
    }
    gevent.spawn(self._SendLoop)
    gevent.spawn(self._RecvLoop)
    gevent.spawn(self._PingLoop)

  def _SendLoop(self):
    while True:
      try:
        payload = self._send_queue.get()
        self._socket.write(payload)
      except Exception as e:
        self._exception = e
        self._state = DispatcherState.FAULTED
        self.Shutdown()
        break

  def _RecvLoop(self):
    while True:
      try:
        sz, = unpack('!i', self._socket.readAll(4))
        buf = StringIO(self._socket.readAll(sz))
        gevent.spawn(self._RecvMessage, buf)
      except Exception as e:
        self._exception = e
        self._state = DispatcherState.FAULTED
        self.Shutdown()
        break

  def _PingLoop(self):
    while True:
      gevent.sleep(30)
      self._SendPingMessage()

  @staticmethod
  def _ReadHeader(msg):
    header, = unpack('!i', msg.read(4))
    msg_type = (256 - (header >> 24 & 0xff)) * -1
    tag = ((header << 8) & 0xFFFFFFFF) >> 8
    return msg_type, tag

  def _RecvMessage(self, msg):
    msg_type, tag = self._ReadHeader(msg)
    msg_cls = None
    processing_excr = None
    try:
      msg_cls = self._handlers[msg_type](msg)
    except Exception as e:
      processing_excr = e
    finally:
      ar = self._ReleaseTag(tag)
      if ar:
        if processing_excr:
          ar.set_exception(DispatcherException('An exception occured while processing a message.', processing_excr))
        elif msg_cls.err:
          ar.set_exception(ServerException(msg_cls.err))
        else:
          ar.set(msg_cls)

  def _ReleaseTag(self, tag):
    ar = self._tag_map.pop(tag, None)
    self._tag_pool.release(tag)
    return ar

  def SendDispatchMessage(self, thrift_payload, timeout=None):
    ctx = {}
    if timeout:
      ctx['com.twitter.finagle.Deadline'] = Deadline(timeout)

    disp_msg = DispatchMessage(thrift_payload, ctx)
    ar = self._SendMessage(disp_msg, timeout)
    return ar

  def _SendPingMessage(self):
    ping_msg = PingMessage()
    return self._SendMessage(ping_msg)

  def _SendMessage(self, msg, timeout=None, oneway=False):
    if self._state != DispatcherState.RUNNING:
      raise Exception("Dispatcher is not in a state to accept messages.")

    if oneway:
      msg.tag = 0
    else:
      msg.tag = self._tag_pool.get()

    fpb = StringIO()
    msg.Marshal(fpb)
    finagle_payload = fpb.getvalue()

    if not oneway:
      ar = AsyncResult()
      self._tag_map[msg.tag] = ar
      self._send_queue.put(finagle_payload)
      if timeout:
        self._InitTimeout(ar, timeout, msg.tag)
    else:
      ar = None

    return ar

  def _TimeoutHelper(self, ar, timeout, tag):
    ar.wait(timeout)
    if not ar.ready():
      ar.set_exception(TimeoutException('The thrift call did not complete within the specified timeout and has been aborted.'))
      msg = DiscardedMessage(tag, 'timeout')
      self._SendMessage(msg, oneway=True)

  def _InitTimeout(self, ar, timeout, tag):
    if timeout:
      gevent.spawn(self._TimeoutHelper, ar, timeout, tag)

  def Shutdown(self):
    # Shutdown all pending calls
    for ar in self._tag_map.values():
      ar.set_exception(DispatcherException("The dispatcher is shutting down."))