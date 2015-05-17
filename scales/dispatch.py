"""Core classes for dispatching messages from a Scales proxy to a message sink stack."""

from gevent.event import AsyncResult

from scales.message import (
  Deadline,
  OneWaySendCompleteMessage,
  RdispatchMessage,
  RerrorMessage,
  SystemErrorMessage,
  TdispatchMessage,
  Timeout,
  TimeoutMessage,)
from scales.sink import ReplySink

class InternalError(Exception): pass
class ServerError(Exception):  pass
class TimeoutError(Exception): pass
class ShutdownError(Exception): pass

class GeventMessageTerminatorSink(ReplySink):
  """A ReplySink that converts a Message into an AsyncResult.
  This sink terminates the reply chain.
  """
  def __init__(self):
    super(GeventMessageTerminatorSink, self).__init__()
    self._ar = AsyncResult()

  def ProcessReturnMessage(self, msg):
    """Convert a Message into an AsyncResult.
    Returns no result and throws no exceptions.

    Args:
      msg - The reply message.
    """
    if isinstance(msg, RdispatchMessage):
      if msg.error_message:
        self._ar.set_exception(ServerError(msg.error_message))
      else:
        self._ar.set(msg.response)
    elif isinstance(msg, RerrorMessage):
      self._ar.set_exception(msg.error_message)
    elif isinstance(msg, TimeoutMessage):
      self._ar.set_exception(TimeoutError(
        'The call did not complete within the specified timeout '
        'and has been aborted.'))
    elif isinstance(msg, SystemErrorMessage):
      self._ar.set_exception(msg.error)
    elif isinstance(msg, OneWaySendCompleteMessage):
      self._ar.set(None)
    else:
      self._ar.set_exception(InternalError('Unknown response message of type %s' % msg.__class__))

  @property
  def async_result(self):
    return self._ar

class MessageDispatcher(object):
  """Handles dispatching incoming and outgoing messages to a client sink stack."""

  def __init__(
        self,
        client_stack_builder,
        dispatch_timeout=10):
    """
    Args:
      client_stack_builder - A ClientMessageSinkStackBuilder
      timeout - The default timeout in seconds for any dispatch messages.
    """
    self._client_stack_builder = client_stack_builder
    self._dispatch_timeout = dispatch_timeout

  def DispatchMethodCall(self, service, method, args, kwargs, timeout=None):
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
    timeout = timeout or self._dispatch_timeout
    # TODO: move this into the mux timeout sink?
    ctx = {}
    if timeout:
      ctx['com.twitter.finagle.Deadline'] = Deadline(timeout)

    disp_msg = TdispatchMessage(service, method, args, kwargs, ctx)
    disp_msg.properties[Timeout.KEY] = timeout

    message_sink = self._client_stack_builder.CreateSinkStack()
    ar_sink = GeventMessageTerminatorSink()
    message_sink.AsyncProcessMessage(disp_msg, ar_sink)
    return ar_sink.async_result if ar_sink else None