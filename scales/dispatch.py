from gevent.event import AsyncResult

from scales.message import (
  Deadline,
  SystemErrorMessage,
  RdispatchMessage,
  RerrorMessage,
  TdispatchMessage,
  Timeout,
  TimeoutMessage,)
from scales.sink import SyncMessageSink

class ServerException(Exception):  pass
class TimeoutException(Exception): pass
class ShutdownException(Exception): pass

class GeventMessageTerminatorSink(SyncMessageSink):
  def __init__(self):
    super(GeventMessageTerminatorSink, self).__init__()
    self._ar = AsyncResult()

  def SyncProcessMessage(self, msg):
    if isinstance(msg, RdispatchMessage):
      if msg.error:
        self._ar.set_exception(ServerException(msg.error))
      else:
        self._ar.set(msg.response)
    elif isinstance(msg, RerrorMessage):
      self._ar.set_exception(msg.error)
    elif isinstance(msg, TimeoutMessage):
      self._ar.set_exception(TimeoutException(
        'The call did not complete within the specified timeout '
        'and has been aborted.'))
    elif isinstance(msg, SystemErrorMessage):
      self._ar.set_exception(msg.exception)
    else:
      self._ar.set_exception(SystemErrorMessage('Unknown response message'))

  @property
  def async_result(self):
    return self._ar

class MessageDispatcher(object):
  """Handles dispatching incoming and outgoing messages to a socket.
  """
  def __init__(
        self,
        client_stack_builder,
        dispatch_timeout=10):
    """
    Args:
      socket - An instance of a TSocket or greater.  THealthySocket is prefered.
      timeout - The default timeout in seconds for any dispatch messages.
    """
    self._client_stack_builder = client_stack_builder
    self._dispatch_timeout = dispatch_timeout

  def DispatchMethodCall(self, service, method, args, timeout=None):
    """Creates and posts a Tdispatch message to a client sink stack.

    Args:
      service - The thrift client class originating this call.
      method  - The method being called.
      args    - The parameters passed to the method.
      timeout - An optional timeout.  If not set, the global dispatch timeout
                will be applied.

    Returns:
      An AsyncResult representing the status of the method call.  This will be
      signaled when either the call completes (successfully or from failure),
      or after [timeout] seconds ellapse.
    """
    timeout = timeout or self._dispatch_timeout
    ctx = {}
    if timeout:
      ctx['com.twitter.finagle.Deadline'] = Deadline(timeout)

    disp_msg = TdispatchMessage(service, method, args, ctx)
    disp_msg.properties[Timeout.KEY] = timeout

    message_sink = self._client_stack_builder.CreateSinkStack()
    ar_sink = GeventMessageTerminatorSink()
    message_sink.AsyncProcessMessage(disp_msg, ar_sink)
    return ar_sink.async_result if ar_sink else None