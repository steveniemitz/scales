from struct import unpack
from cStringIO import StringIO

import gevent
from gevent.event import AsyncResult

from ttypes import (
  DispatcherState,
  MessageType)
from message import (
  Deadline,
  TdiscardedMessage,
  TdispatchMessage,
  Message,
  MessageSerializer,
  RMessage,
  RdispatchMessage,
  RerrorMessage,
  TpingMessage)

import sink


class MessageDispatcher(object):
  """Handles dispatching incoming and outgoing messages to a socket.
  """
  def __init__(
        self,
        transport_provider,
        client_stack_builder,
        dispatch_timeout=10):
    """
    Args:
      socket - An instance of a TSocket or greater.  THealthySocket is prefered.
      timeout - The default timeout in seconds for any dispatch messages.
    """
    self._state = DispatcherState.UNINITIALIZED
    self._open_result = None
    self._transport_provider = transport_provider
    self._client_stack_builder = client_stack_builder
    self._exception = None
    self._ping_timeout = 5
    self._ping_ar = None
    self._serializer = MessageSerializer()
    self._shutdown_ar = AsyncResult()
    self._dispatch_timeout = dispatch_timeout
    self._transport_sink = None

  @property
  def state(self):
    """The current state of the dispatcher.
    """
    return self._state

  @property
  def isRunning(self):
    """Returns true if state == RUNNING
    """
    return self.state == DispatcherState.RUNNING

  @property
  def shutdown_result(self):
    """An instance of AsyncResult representing the state of the dispatcher.
    When the dispatcher shuts down, this result will be signaled.
    """
    return self._shutdown_ar

  def _PingLoop(self):
    """Periodically pings the remote server.
    """
    while True:
      gevent.sleep(30)
      if self.isRunning:
        ar = self._SendPingMessage()
        ar.rawlink(self._OnPingResponse)
        self._ping_ar = ar
      else:
        break

  def _CreateSinkStack(self):
    sinks = [
      sink.ThriftMuxDispatchSink(),
      sink.ThrfitMuxMessageSerializerSink(),
      self._transport_sink
    ]
    for s in range(0, len(sinks) - 1):
      sinks[s].next_sink = sinks[s + 1]

    return sinks[0]

  def SendDispatchMessage(self, thrift_payload, timeout=None):
    """Creates and posts a Tdispatch message to the send queue.

    Args:
      thrift_payload - A raw serialized thirft method call payload.  Note: This
                       must NOT be framed.
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

    disp_msg = TdispatchMessage(thrift_payload, ctx)
    ar = self._SendMessage(disp_msg, timeout)
    return ar

  def _SendPingMessage(self):
    """Constucts and sends a Tping message.
    """
    ping_msg = TpingMessage()
    return self._SendMessage(ping_msg, self._ping_timeout)

  def _OnPingResponse(self, ar):
    """Handles the response to a ping.  On failure, shuts down the dispatcher.
    """
    if not ar.successful():
      self._exception = ar.exception
      self._Shutdown(DispatcherState.PING_TIMEOUT)

  def _SendMessage(self, msg, timeout=None, oneway=False):
    """Send a message to the remote host.

    Args:
      msg - The message object to serialize and send.
      timeout - An optional timeout for the response.
      oneway - If true, no response is expected and no event is allocated for
               the response.  Note: messages with oneway = True always have
               tag = 0.
    Returns:
      An AsyncResult representing the server response, or None if oneway=True.
    """
    if not (self.isRunning or self.state == DispatcherState.STARTING):
      raise Exception("Dispatcher is not in a state to accept messages.")

    sink_stack = self._CreateSinkStack()
    ar_sink = None
    if not oneway:
      ar_sink = sink.GeventReplySink()
    sink_stack.AsyncProcessMessage(msg, None, ar_sink)

    return ar_sink._ar

  def _Shutdown(self, termainal_state, excr=None):
    """Shutdown the dispatcher, no more requests will be accepted after.

    Args:
      terminal_state - The final state the dispatcher should be in after shutdown.
      excr - The exception to record as the shutdown reason.
    """
    self._state = termainal_state
    # Shutdown all pending calls
    for ar in self._tag_map.values():
      ar.set_exception(DispatcherException("The dispatcher is shutting down."))
    self._shutdown_ar.set(excr or self._exception)

  def isOpen(self):
    """Returns true if open() has been called on the dispatcher.
    """
    return self.isRunning

  def open(self):
    """Initializes the dispatcher, opening a connection to the remote host.
    This method may only be called once.
    """
    if self.state == DispatcherState.RUNNING:
      return
    elif self._open_result:
      self._open_result.get()
      return

    self._open_result = AsyncResult()
    try:
      self._state = DispatcherState.STARTING
      self._transport_sink = self._transport_provider.GetTransportSink()
      try:
        ar = self._SendPingMessage()
        # Block the open() call until we get a ping response back.
        ar.get()
      except Exception as e:
        #self._Shutdown(DispatcherState.FAULTED, e)
        raise

      self._state = DispatcherState.RUNNING
      gevent.spawn(self._PingLoop)
      self._open_result.set('open')

    except Exception as e:
      self._open_result.set_exception(e)
      raise

  def close(self):
    """Close the dispatcher, shutting it down and denying any future requests
    over it.
    """
    self._Shutdown(DispatcherState.STOPPED)

  def testConnection(self):
    """Test the dispatcher's connection to the remote server.  Returns true if
    the dispatcher is running and (if supported) the underlying socket's
    testConnection() returns True.
    """
    if not self.isRunning:
      return False
    return True
