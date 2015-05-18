"""Core classes for dispatching messages from a Scales proxy to a message sink stack."""

import functools

from gevent.event import AsyncResult

from scales.message import (
  OneWaySendCompleteMessage,
  RdispatchMessage,
  RerrorMessage,
  ScalesErrorMessage,
  TdispatchMessage,
  Timeout,
  TimeoutMessage,)
from scales.sink import ReplySink
from scales.varz import VarzReceiver

class InternalError(Exception): pass
class ScalesError(Exception):  pass
class TimeoutError(Exception): pass

class GeventMessageTerminatorSink(ReplySink):
  """A ReplySink that converts a Message into an AsyncResult.
  This sink terminates the reply chain.
  """
  def __init__(self, source):
    super(GeventMessageTerminatorSink, self).__init__()
    self._ar = AsyncResult()
    self._source = source

  @staticmethod
  def _MakeServerException(system_msg):
    """Creates an exception object that contains the inner exception
    from a SystemErrorMessage.  This allows the actual failure stack
    to propegate to the waiting greenlet.
    """
    if system_msg.stack:
      msg = """An error occured while processing the request:
[Inner Exception: --------------------]
%s[End of Inner Exception---------------]
""" % system_msg.stack
      return ScalesError(msg)
    else:
      return system_msg.error

  def ProcessReturnMessage(self, msg):
    """Convert a Message into an AsyncResult.
    Returns no result and throws no exceptions.

    Args:
      msg - The reply message.
    """
    MessageDispatcher.Varz.response_messages(self._source)
    if isinstance(msg, RdispatchMessage):
      if msg.error_message:
        self._ar.set_exception(ScalesError(msg.error_message))
      else:
        self._ar.set(msg.response)
    elif isinstance(msg, RerrorMessage):
      self._ar.set_exception(msg.error_message)
    elif isinstance(msg, TimeoutMessage):
      self._ar.set_exception(TimeoutError(
        'The call did not complete within the specified timeout '
        'and has been aborted.'))
    elif isinstance(msg, ScalesErrorMessage):
      self._ar.set_exception(self._MakeServerException(msg))
    elif isinstance(msg, OneWaySendCompleteMessage):
      self._ar.set(None)
    else:
      self._ar.set_exception(InternalError('Unknown response message of type %s' % msg.__class__))

  @property
  def async_result(self):
    return self._ar

class MessageDispatcher(object):
  """Handles dispatching incoming and outgoing messages to a client sink stack."""
  class Varz(object):
    dispatch_messages = functools.partial(
        VarzReceiver.IncrementVarz,
        metric='scales.MessageDispatcher.dispatch_messages',
        amount=1)
    response_messages = functools.partial(
        VarzReceiver.IncrementVarz,
        metric='scales.MessageDispatcher.response_messages',
        amount=1)

  def __init__(
        self,
        service,
        client_stack_builder,
        dispatch_timeout=10):
    """
    Args:
      service - The service interface class this dispatcher is serving.
      client_stack_builder - A ClientMessageSinkStackBuilder
      timeout - The default timeout in seconds for any dispatch messages.
    """
    self._client_stack_builder = client_stack_builder
    self._dispatch_timeout = dispatch_timeout
    self._service = service

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
    timeout = timeout or self._dispatch_timeout
    disp_msg = TdispatchMessage(self._service, method, args, kwargs)
    disp_msg.properties[Timeout.KEY] = timeout

    message_sink = self._client_stack_builder.CreateSinkStack()
    source = '%s.%s' % (self._service, method)
    ar_sink = GeventMessageTerminatorSink(source)
    self.Varz.dispatch_messages(source)
    message_sink.AsyncProcessMessage(disp_msg, ar_sink)
    return ar_sink.async_result if ar_sink else None