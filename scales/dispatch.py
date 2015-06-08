"""Core classes for dispatching messages from a Scales proxy to a message sink stack."""

import time

import gevent
from gevent.event import AsyncResult

from .constants import SinkProperties
from .message import (
  Deadline,
  MethodCallMessage,
  MethodReturnMessage,
  TimeoutError
)
from .sink import (
  ClientMessageSink,
  ClientMessageSinkStack
)
from .varz import (
  Counter,
  SourceType,
  AverageTimer,
  VarzBase
)

class InternalError(Exception): pass
class ScalesError(Exception):  pass
class ServiceClosedError(Exception): pass

class MessageDispatcher(ClientMessageSink):
  """Handles dispatching incoming and outgoing messages to a client sink stack."""
  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.MessageDispatcher'
    _VARZ_SOURCE_TYPE = SourceType.MethodAndService
    _VARZ = {
      'dispatch_messages': Counter,
      'success_messages': Counter,
      'exception_messages': Counter,
      'request_latency': AverageTimer
    }

  def __init__(
        self,
        service,
        sink_provider,
        properties):
    """
    Args:
      service - The service interface class this dispatcher is serving.
      client_stack_builder - A ClientMessageSinkStackBuilder
      timeout - The default timeout in seconds for any dispatch messages.
    """
    super(MessageDispatcher, self).__init__()
    self._sink_provider = sink_provider
    self.next_sink = sink_provider.CreateSink(properties)
    self._dispatch_timeout = properties[SinkProperties.Timeout]
    self._service = service
    self._name = properties[SinkProperties.Service]
    self._open_ar = AsyncResult()

  def Open(self, force=False):
    self._open_ar = self.next_sink.Open()
    return self._open_ar

  def Close(self):
    self.next_sink.Close()
    self._open_ar = None

  def DispatchMethodCall(self, method, args, kwargs, timeout=None):
    """Creates and posts a Tdispatch message to a client sink stack.

    Args:
      service - The service interface originating this call.
      method  - The method being called.
      args    - The parameters passed to the method.
      kwargs  - The keyword parameters passed to the method.
      timeout - An optional timeout.  If not set, the global dispatch timeout
                will be applied.

    Returns:
      An AsyncResult representing the status of the method call.  This will be
      signaled when either the call completes (successfully or from failure),
      or after [timeout] seconds elapse.
    """
    if not self._open_ar:
      raise Exception('Dispatcher not open.')

    timeout = timeout or self._dispatch_timeout
    now = time.time()
    self._open_ar.wait(timeout)

    disp_msg = MethodCallMessage(self._service, method, args, kwargs)
    if timeout:
      deadline = now + timeout
      disp_msg.properties[Deadline.KEY] = deadline

    source = method, self._name, None
    self.Varz.dispatch_messages(source) # pylint: disable=no-member

    ar = AsyncResult()
    sink_stack = ClientMessageSinkStack()
    sink_stack.Push(self, (source, time.time(), ar))
    gevent.spawn(self.next_sink.AsyncProcessRequest, sink_stack, disp_msg, None, {})
    return ar

  @staticmethod
  def _WrapException(msg):
    """Creates an exception object that contains the inner exception
    from a SystemErrorMessage.  This allows the actual failure stack
    to propagate to the waiting greenlet.

    Args:
      msg - The MethodReturnMessage that has an active error.

    Returns:
      An exception object wrapping the error in the MethodCallMessage.
    """
    # Don't wrap timeouts.
    if msg.error is TimeoutError:
      return msg.error

    stack = getattr(msg, 'stack', None)
    if stack:
      msg = """An error occurred while processing the request
[Inner Exception: --------------------]
%s[End of Inner Exception---------------]
""" % ''.join(stack)
      return ScalesError(msg)
    else:
      return msg.error

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    raise NotImplementedError("This should never be called.")

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    """Convert a Message into an AsyncResult.
      Returns no result and throws no exceptions.

    Args:
      msg - The reply message (a MethodReturnMessage).
    """
    source, start_time, ar = context
    end_time = time.time()
    self.Varz.request_latency(source, end_time - start_time) # pylint: disable=no-member
    if isinstance(msg, MethodReturnMessage):
      if msg.error:
        self.Varz.exception_messages(source) # pylint: disable=no-member
        ar.set_exception(self._WrapException(msg))
      else:
        self.Varz.success_messages(source) # pylint: disable=no-member
        ar.set(msg.return_value)
    else:
      ar.set_exception(InternalError('Unknown response message of type %s'
                                           % msg.__class__))
