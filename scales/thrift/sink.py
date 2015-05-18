from struct import (pack, unpack)
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult

from ..message import (
  MethodCallMessage,
  MethodReturnMessage,
  ScalesClientError,
  Timeout,
  TimeoutError
)
from ..sink import (
  ClientChannelSink,
  ClientChannelSinkStack,
  ClientFormatterSink,
)
from .formatter import MessageSerializer

class NoopTimeout(object):
  def start(self): pass
  def cancel(self): pass

class SocketTransportSink(ClientChannelSink):
  def __init__(self, socket, source):
    super(SocketTransportSink, self).__init__()
    self._socket = socket
    self._shutdown_ar = AsyncResult()

  @property
  def shutdown_result(self):
    return self._shutdown_ar

  def _Shutdown(self, reason):
    if not self.isOpen():
      return
    self._socket.close()
    if not isinstance(reason, Exception):
      reason = Exception(str(reason))
    self._shutdown_ar.set_exception(reason)

  def _AsyncProcessTransaction(self, data, sink_stack, timeout):
    gtimeout = gevent.Timeout.start_new(timeout) if timeout else NoopTimeout()
    try:
      self._socket.write(data)
      sz, = unpack('!i', self._socket.readAll(4))
      buf = StringIO(self._socket.readAll(sz))
      gtimeout.cancel()
      gevent.spawn(self._ProcessReply, buf, sink_stack)
    except gevent.Timeout:
      err = TimeoutError()
      self._socket.close()
      sink_stack.DispatchReplyMessage(MethodReturnMessage(error=err))
    except Exception as ex:
      gtimeout.cancel()
      self._Shutdown(ex)
      sink_stack.DispatchReplyMessage(MethodReturnMessage(error=ex))

  @staticmethod
  def _ProcessReply(buf, sink_stack):
    try:
      buf.seek(0)
      sink_stack.AsyncProcessResponse(buf)
    except Exception as ex:
      sink_stack.DispatchReplyMessage(MethodReturnMessage(error=ex))

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    payload = stream.getvalue()
    sz = pack('!i', len(payload))

    timeout = msg.properties.get(Timeout.KEY)
    gevent.spawn(self._AsyncProcessTransaction, sz + payload, sink_stack, timeout)
    pass

  def AsyncProcessResponse(self, sink_stack, context, stream):
    raise NotImplementedError("This should never be called.")

  def isOpen(self):
    return self._socket.isOpen()

  def open(self):
    self._socket.open()

  def close(self):
    self._socket.close()

  def testConnection(self):
    return self._socket.testConnection()


class ThriftFormatterSink(ClientFormatterSink):
  def __init__(self, source):
    super(ThriftFormatterSink, self).__init__()

  def AsyncProcessMessage(self, msg, reply_sink):
    buf = StringIO()
    headers = {}

    if not isinstance(msg, MethodCallMessage):
      reply_sink.ProcessReturnMessage(MethodReturnMessage(
          error=ScalesClientError('Invalid message type.')))
    try:
      MessageSerializer.SerializeThriftCall(msg, buf)
      ctx = msg.service, msg.method
    except Exception as ex:
      reply_sink.ProcessReturnMessage(MethodReturnMessage(error=ex))
      return

    sink_stack = ClientChannelSinkStack(reply_sink)
    sink_stack.Push(self, ctx)
    self.next_sink.AsyncProcessRequest(sink_stack, msg, buf, headers)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    raise NotImplementedError('This should never be called.')

  def AsyncProcessResponse(self, sink_stack, context, stream):
    try:
      msg = MessageSerializer.DeserializeThriftCall(stream, context)
    except Exception as ex:
      msg = MethodReturnMessage(error=ex)
    sink_stack.DispatchReplyMessage(msg)
