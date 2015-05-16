import traceback

from collections import deque
from struct import (pack, unpack)
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult, Event
from gevent.queue import Queue

from scales.message import (
    RdispatchMessage,
    RerrorMessage,
    TdiscardedMessage,
    MessageSerializer,
    TimeoutMessage,
    Timeout,
    Tag)

class ServerException(Exception):  pass
class TimeoutException(Exception): pass


class MessageSink(object):
  def __init__(self):
    super(MessageSink, self).__init__()
    self._next = None

  @property
  def next_sink(self):
    return self._next

  @next_sink.setter
  def next_sink(self, value):
    self._next = value


class SyncMessageSink(MessageSink):
  def __init__(self):
    super(SyncMessageSink, self).__init__()

  def SyncProcessMessage(self, msg):
    raise NotImplementedError()


class AsyncMessageSink(MessageSink):
  def __init__(self):
    super(AsyncMessageSink, self).__init__()

  def AsyncProcessMessage(self, msg, reply_sink):
    raise NotImplementedError()


class ClientChannelSink(object):
  def __init__(self):
    super(ClientChannelSink, self).__init__()

  def AsyncProcessRequest(self, sink_stack, msg, stream):
    raise NotImplementedError()

  def AsyncProcessResponse(self, sink_stack, stream):
    raise NotImplementedError()


class ClientFormatterSink(AsyncMessageSink, ClientChannelSink):
  def __init__(self):
    super(ClientFormatterSink, self).__init__()


class SinkStack(object):
  def __init__(self):
    self._stack = deque()

  def Push(self, sink):
    self._stack.append(sink)

  def Pop(self):
    return self._stack.pop()


class ClientChannelSinkStack(SinkStack):
  def __init__(self, reply_sink):
    super(ClientChannelSinkStack, self).__init__()
    self._reply_sink = reply_sink

  def AsyncProcessResponse(self, stream):
    if self._reply_sink:
      sink = self.Pop()
      sink.AsyncProcessResponse(self, stream)

  def DispatchReplyMessage(self, msg):
    if self._reply_sink:
      self._reply_sink.SyncProcessMessage(msg)


class MessageProcessing(object):
  def Cancel(self):
    pass


