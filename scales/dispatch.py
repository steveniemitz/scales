"""Core classes for dispatching messages from a Scales proxy to a message sink stack."""

import time

import gevent

from .async import AsyncResult
from .constants import MessageProperties, SinkProperties
from .message import (
  Deadline,
  MethodCallMessage,
  MethodReturnMessage,
  TimeoutError
)
from .sink import (
  ClientMessageSink,
  MessageSinkStack
)
from .varz import (
  Rate,
  Source,
  AverageTimer,
  VarzBase
)

class InternalError(Exception): pass
class ScalesError(Exception):
  def __init__(self, ex, msg):
    self.inner_exception = ex
    super(ScalesError, self).__init__(msg)

class ServiceClosedError(Exception): pass

class _AsyncResponseSink(ClientMessageSink):
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
    if isinstance(msg.error, TimeoutError):
      return msg.error

    stack = getattr(msg, 'stack', None)
    if stack:
      ex_msg = """An error occurred while processing the request
[Inner Exception: --------------------]
%s[End of Inner Exception---------------]
""" % ''.join(stack)
      return ScalesError(msg.error, ex_msg)
    else:
      return msg.error

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    raise NotImplementedError("This should never be called")

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    """Propagate the results from a message onto an AsyncResult.

    Args:
      msg - The reply message (a MethodReturnMessage).
    """
    source, start_time, ar, msg_props = context
    endpoint = msg_props.get(MessageProperties.Endpoint, None)
    if endpoint and not isinstance(endpoint, str):
      endpoint = str(endpoint)
    if source:
      host_source = Source(method=source.method,
        service=source.service,
        endpoint=endpoint)
    else:
      host_source = None

    end_time = time.time()
    if host_source:
      MessageDispatcher.Varz.request_latency(host_source, end_time - start_time) # pylint: disable=no-member
    if isinstance(msg, MethodReturnMessage):
      if msg.error:
        if host_source:
          MessageDispatcher.Varz.exception_messages(host_source) # pylint: disable=no-member
        ar.set_exception(self._WrapException(msg))
      else:
        if host_source:
          MessageDispatcher.Varz.success_messages(host_source) # pylint: disable=no-member
        ar.set(msg.return_value)
    else:
      ar.set_exception(InternalError('Unknown response message of type %s'
                                     % msg.__class__))

class MessageDispatcher(ClientMessageSink):
  """Handles dispatching incoming and outgoing messages to a client sink stack."""

  class Varz(VarzBase):
    """
    dispatch_messages - The number of messages dispatched.
    success_messages - The number of successful responses processed.
    exception_messages - The number of exception responses processed.
    request_latency - The average time taken to receive a response to a request.
    """
    _VARZ_BASE_NAME = 'scales.MessageDispatcher'
    _VARZ = {
      'dispatch_messages': Rate,
      'success_messages': Rate,
      'exception_messages': Rate,
      'request_latency': AverageTimer
    }

  def __init__(
        self,
        service,
        sink_provider,
        default_timeout,
        properties):
    """
    Args:
      service - The service interface class this dispatcher is serving.
      sink_provider - An instance of a SinkProvider.
      properties - The properties associated with this service and dispatcher.
    """
    super(MessageDispatcher, self).__init__()
    self.next_sink = sink_provider.CreateSink(properties)
    self._dispatch_timeout = default_timeout
    self._service = service
    self._name = properties[SinkProperties.Label]
    self._open_ar = AsyncResult()

  def Open(self):
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
    start_time = time.time()
    if self._open_ar.ready():
      return self._DispatchMethod(method, args, kwargs, timeout, start_time)
    else:
      # _DispatchMethod returns an AsyncResult, so we end up with an
      # AsyncResult<AsyncResult<TRet>>, Unwrap() removes one layer, yielding
      # an AsyncResult<TRet>
      return self._open_ar.ContinueWith(
          lambda ar: self._DispatchMethod(method, args, kwargs, timeout, start_time)
      ).Unwrap()

  @staticmethod
  def StaticDispatchMessage(sink, source, start_time, deadline, disp_msg):
    # Init the properties dictionary w/ an empty endpoint
    disp_msg.properties[MessageProperties.Endpoint] = None
    if deadline:
      disp_msg.properties[Deadline.KEY] = deadline

    ar = AsyncResult()
    sink_stack = MessageSinkStack()
    repsonse_sink = _AsyncResponseSink()
    sink_stack.Push(repsonse_sink, (source, start_time, ar, disp_msg.properties))
    gevent.spawn(sink.AsyncProcessRequest, sink_stack, disp_msg, None, {})
    return ar

  def _DispatchMethod(self, method, args, kwargs, timeout, start_time):
    open_time = time.time()
    open_latency = open_time - start_time

    if timeout:
      # Calculate the deadline for this method call.
      # Reduce it by the time it took for the open() to complete.
      deadline = start_time + timeout - open_latency
    else:
      deadline = None

    disp_msg = MethodCallMessage(self._service, method, args, kwargs)
    source = Source(method=method, service=self._name)
    self.Varz.dispatch_messages(source) # pylint: disable=no-member
    return self.StaticDispatchMessage(self.next_sink, source, start_time, deadline, disp_msg)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    raise NotImplementedError("This should never be called.")

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise NotImplementedError("This should never be called.")
