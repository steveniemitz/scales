from collections import deque
from scales.message import SystemErrorMessage
import gevent

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

  def AsyncProcessResponse(self, sink_stack, context, stream):
    raise NotImplementedError()


class ClientFormatterSink(AsyncMessageSink, ClientChannelSink):
  def __init__(self):
    super(ClientFormatterSink, self).__init__()


class SinkStack(object):
  def __init__(self):
    self._stack = deque()

  def Push(self, sink, context=None):
    self._stack.append((sink, context))

  def Pop(self):
    return self._stack.pop()


class ClientChannelSinkStack(SinkStack):
  def __init__(self, reply_sink):
    super(ClientChannelSinkStack, self).__init__()
    self._reply_sink = reply_sink

  def AsyncProcessResponse(self, stream):
    sink, context = self.Pop()
    sink.AsyncProcessResponse(self, context, stream)

  def DispatchReplyMessage(self, msg):
    if self._reply_sink:
      self._reply_sink.SyncProcessMessage(msg)


class PoolMemberSelectorTransportSink(ClientChannelSink):
  def __init__(self, pool):
    super(PoolMemberSelectorTransportSink, self).__init__()
    self._pool = pool

  def AsyncProcessRequestCallback(self, sink_stack, msg, stream):
    try:
      shard, sink = self._pool.Get()
    except Exception as e:
      excr = SystemErrorMessage(e)
      sink_stack.DispatchReplyMessage(excr)
      return

    sink_stack.Push(self, (shard, sink))
    return sink.AsyncProcessRequest(sink_stack, msg, stream)

  def AsyncProcessRequest(self, sink_stack, msg, stream):
    gevent.spawn(self.AsyncProcessRequestCallback, sink_stack, msg, stream)

  def AsyncProcessResponse(self, sink_stack, context, stream):
    self._pool.Return(*context)

    next_sink, next_ctx = sink_stack.Pop()
    next_sink.AsyncProcessResponse(sink_stack, context, stream)