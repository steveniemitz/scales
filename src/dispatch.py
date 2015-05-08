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
  RerrorMessage,
  PingMessage)

class ServerException(Exception):  pass
class DispatcherException(Exception): pass
class TimeoutException(Exception): pass

class TagPool(object):
  """A class which manages a pool of tags
  """
  def __init__(self, max_tag):
    self._set = set()
    self._next = 1
    self._max_tag = max_tag

  def get(self):
    """Get a tag from the pool.

    Returns:
      A tag

    Raises:
      Exception if the next tag will be > max_tag
    """
    if not any(self._set):
      if self._next == self._max_tag - 1:
        raise Exception("No tags left in pool.")

      self._next += 1
      return self._next
    else:
      return self._set.pop()

  def release(self, tag):
    """Return a tag to the pool.

    Args:
      tag - The previously leased tag.
    """
    self._set.add(tag)


class MuxMessageDispatcher(object):
  """Handles dispatching incoming and outgoing messages to a socket.
  """
  class Handlers(object):
    """Handlers for recieved messages.

    All handlers take a raw message (in bytes) and return an object.
    """
    @staticmethod
    def Rdispatch(msg):
      """Handle an Rdispatch message (response to a dispatch message.)
      """
      msg_cls = RdispatchMessage.Unmarshal(msg)
      return msg_cls

    @staticmethod
    def Rping(msg):
      """Handle a Rping message (response to a ping.)
      """
      return RMessage(MessageType.Rping)

    @staticmethod
    def Rerr(msg):
      msg_class = RerrorMessage.Unmarshal(msg)
      return msg_class

  def __init__(self, socket, dispatch_timeout=10):
    """
    Args:
      socket - An instance of a TSocket or greater.  THealthySocket is prefered.
      timeout - The default timeout in seconds for any dispatch messages.
    """
    self._state = DispatcherState.UNINITIALIZED
    self._open_result = None
    self._socket = socket
    self._tag_pool = TagPool((2 ** 24) -1)
    self._send_queue = Queue()
    self._tag_map = {}
    self._exception = None
    self._ping_timeout = 5
    self._ping_ar = None
    self._handlers = {
      MessageType.Rdispatch: self.Handlers.Rdispatch,
      MessageType.Rping: self.Handlers.Rping,
      MessageType.Rerr: self.Handlers.Rerr,
      MessageType.BAD_Rerr: self.Handlers.Rerr
    }
    self._shutdown_ar = AsyncResult()
    self._dispatch_timeout = dispatch_timeout

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

  def _SendLoop(self):
    """Dispatch messages from the send queue to the remote server.

    Note: Messages in the queue have already been serialized into wire format.
    """
    while True:
      try:
        payload = self._send_queue.get()
        self._socket.write(payload)
      except Exception as e:
        self._exception = e
        self._Shutdown(DispatcherState.FAULTED)
        break

  def _RecvLoop(self):
    """Dispatch messages from the remote server to their recipient.

    Note: Deserialization and dispatch occurs on a seperate greenlet, this only
    reads the message off the wire.
    """
    while True:
      try:
        sz, = unpack('!i', self._socket.readAll(4))
        buf = StringIO(self._socket.readAll(sz))
        gevent.spawn(self._RecvMessage, buf)
      except Exception as e:
        self._exception = e
        self._Shutdown(DispatcherState.FAULTED)
        break

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

  @staticmethod
  def _ReadHeader(msg):
    """Read a mux header off a message.

    Args:
      msg - a byte buffer of raw data.

    Returns:
      A tuple of (message_type, tag)
    """
    header, = unpack('!i', msg.read(4))
    msg_type = (256 - (header >> 24 & 0xff)) * -1
    tag = ((header << 8) & 0xFFFFFFFF) >> 8
    return msg_type, tag

  def _RecvMessage(self, msg):
    """Read and dispatch a full message from raw data.  This method never throws
    and returns nothing, all dispatch is expected to occur within.

    Args:
      msg - The raw message bytes.
    """
    msg_type, tag = self._ReadHeader(msg)
    msg_cls = None
    processing_excr = None
    try:
      msg_cls = self._handlers[msg_type](msg)
    except Exception as e:
      processing_excr = e
    finally:
      # Tag 0 responses should never occur.
      if tag == 0:
        return

      ar = self._ReleaseTag(tag)
      if ar:
        if processing_excr:
          ar.set_exception(DispatcherException(
              'An exception occured while processing a message.',
              processing_excr))
        elif msg_cls.err:
          ar.set_exception(ServerException(msg_cls.err))
        else:
          ar.set(msg_cls)

  def _ReleaseTag(self, tag):
    """Return a tag to the tag pool.

    Note: Tags are only returned when the server has ACK'd them (or NACK'd) with
    and Rdispatch message (or similar).  Client initiated timeouts do NOT return
    tags to the pool.

    Args:
      tag - The tag to return.

    Returns:
      The AsyncResult associated with the tag's response.
    """
    ar = self._tag_map.pop(tag, None)
    self._tag_pool.release(tag)
    return ar

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

    disp_msg = DispatchMessage(thrift_payload, ctx)
    ar = self._SendMessage(disp_msg, timeout)
    return ar

  def _SendPingMessage(self):
    """Constucts and sends a Tping message.
    """
    ping_msg = PingMessage()
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
    if not self.isRunning:
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
    """Waits for ar to be signaled or [timeout] seconds to elapse.  If the
    timeout elapses, a Tdiscarded message will be queued to the server indicating
    the client is no longer expecting a reply.
    """
    ar.wait(timeout)
    if not ar.ready():
      ar.set_exception(TimeoutException('The thrift call did not complete within the specified timeout and has been aborted.'))
      msg = DiscardedMessage(tag, 'timeout')
      self._SendMessage(msg, oneway=True)

  def _InitTimeout(self, ar, timeout, tag):
    """Initialize the timeout handler for this request.

    Args:
      ar - The AsyncResult for the pending response of this request.
      timeout - An optional timeout.  If None, no timeout handler is initialized.
      tag - The tag of the request.
    """
    if timeout:
      gevent.spawn(self._TimeoutHelper, ar, timeout, tag)

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
      self._socket.open()
      gevent.spawn(self._SendLoop)
      gevent.spawn(self._RecvLoop)
      self._state = DispatcherState.RUNNING
      gevent.spawn(self._PingLoop)

      ar = self._SendPingMessage()
      # Block the open() call until we get a ping response back.
      ar.get()
      self._open_result.set('open')

    except Exception as e:
      self._open_result.set_exception(e)

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
    if hasattr(self._socket, 'testConnection'):
      return self._socket.testConnection()
    else:
      return True
