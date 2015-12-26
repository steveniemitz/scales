from abc import abstractmethod

import logging
import time
from struct import unpack
from cStringIO import StringIO

import gevent
from gevent.queue import Queue

from ..async import (
  AsyncResult,
  NamedGreenlet
)
from ..constants import ChannelState, ConnectionRole
from ..message import (
  Deadline,
  ClientError,
  MethodReturnMessage
)
from ..sink import (
  ClientMessageSink,
)
from ..varz import (
  AggregateTimer,
  AverageTimer,
  Counter,
  Gauge,
  Rate,
  Source,
  VarzBase
)
from ..constants import TransportHeaders

ROOT_LOG = logging.getLogger('scales.mux')

class Tag(object):
  KEY = "__Tag"

  def __init__(self, tag):
    self._tag = tag

  def Encode(self):
    return [self._tag >> 16 & 0xff,
            self._tag >>  8 & 0xff,
            self._tag       & 0xff]


class TagPool(object):
  """A class which manages a pool of tags
    """
  POOL_LOGGER = ROOT_LOG.getChild('TagPool')

  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.mux.TagPool'
    _VARZ = {
      'pool_exhausted': Counter,
      'max_tag': Gauge
    }

  def __init__(self, max_tag, service, host):
    self._set = set()
    self._next = 1
    self._max_tag = max_tag
    self._varz = self.Varz(Source(service=service, endpoint=host))
    self._log = self.POOL_LOGGER.getChild('[%s.%s]' % (service, host))

  def get(self):
    """Get a tag from the pool.

    Returns:
      A tag

    Raises:
      Exception if the next tag will be > max_tag
    """
    if not self._set:
      if self._next == self._max_tag - 1:
        self._varz.pool_exhausted()
        raise Exception("No tags left in pool.")
      self._next += 1
      ret_tag = self._next
      self._log.debug('Allocating new tag, max is now %d' % ret_tag)
      self._varz.max_tag(ret_tag)
      return ret_tag
    else:
      return self._set.pop()

  def release(self, tag):
    """Return a tag to the pool.

    Args:
      tag - The previously leased tag.
    """
    if tag in self._set:
      self._log.warning('Tag %d has been returned more than once!' % tag)
    self._set.add(tag)


class MuxSocketTransportSink(ClientMessageSink):
  """A transport sink for tmux servers.
  This sink supports concurrent requests over its transport.
  """

  SINK_LOG = ROOT_LOG.getChild('SocketTransportSink')
  _EMPTY_DCT = {}
  _CLOSE_INVOKED = "Close invoked"

  class Varz(VarzBase):
    """
    messages_sent - The number of messages sent over this sink.
    messages_recv - The number of messages received over this sink.
    active - 1 if the sink is open, else 0.
    send_queue_size - The length of the send queue.
    send_time - The aggregate amount of time spent sending data.
    recv_time - The aggregate amount of time spend receiving data.
    send_latency - The average amount of time taken to send a message.
    recv_latency - The average amount of time taken to receive a message
                   (once a response has reached the client).
    transport_latency - The average amount of time taken to perform a full
                        method call transaction (send data, wait for response,
                        read response).
    """
    _VARZ_BASE_NAME = 'scales.thriftmux.SocketTransportSink'
    _VARZ = {
      'messages_sent': Rate,
      'messages_recv': Rate,
      'active': Gauge,
      'send_queue_size': Gauge,
      'send_time': AggregateTimer,
      'recv_time': AggregateTimer,
      'send_latency': AverageTimer,
      'recv_latency': AverageTimer,
      'transport_latency': AverageTimer
    }

  def __init__(self, socket, service, next_provider, sink_properties, global_properties, connection_type=ConnectionRole.Client):
    super(MuxSocketTransportSink, self).__init__()
    self._socket = socket
    self._state = ChannelState.Idle
    self._log = self.SINK_LOG.getChild('[%s.%s:%d]' % (
      service, self._socket.host, self._socket.port))
    self._socket_source = '%s:%d' % (self._socket.host, self._socket.port)
    self._service = service
    self._open_result = None
    self._connection_type = connection_type
    self._varz = self.Varz(Source(service=self._service,
      endpoint=self._socket_source))

  def _Init(self):
    self._tag_map = {}
    self._open_result = None
    self._tag_pool = TagPool((2 ** 24) - 1, self._service, self._socket_source)
    self._greenlets = []
    self._send_queue = Queue()

  @property
  def isActive(self):
    return self._state != ChannelState.Closed

  @property
  def state(self):
    return self._state

  def Open(self):
    """Initializes the dispatcher, opening a connection to the remote host.
    This method may only be called once.
    """
    if not self._open_result:
      self._Init()
      self._open_result = AsyncResult()
      self._open_result.SafeLink(self._OpenImpl)
    return self._open_result

  def _SpawnNamedGreenlet(self, name, *args, **kwargs):
    return NamedGreenlet.spawn(
      'Scales %s for %s [%s]' % (name, self._service, self._socket_source),
      *args,
      **kwargs)

  def _OpenImpl(self):
    try:
      self._log.debug('Opening transport.')
      if self._connection_type == ConnectionRole.Client:
        self._socket.open()
      self._greenlets.append(self._SpawnNamedGreenlet('Recv Loop', self._RecvLoop))
      self._greenlets.append(self._SpawnNamedGreenlet('Send Loop', self._SendLoop))

      self._CheckInitialConnection()
      self._log.debug('Open successful')
      self._state = ChannelState.Open
      self._varz.active(1)
    except Exception as e:
      self._log.error('Exception opening socket')
      self._open_result.set_exception(e)
      self._Shutdown('Open failed')
      raise

  @abstractmethod
  def _CheckInitialConnection(self):
    raise NotImplementedError()

  def Close(self):
    self._Shutdown(self._CLOSE_INVOKED, False)

  def _Shutdown(self, reason, fault=True):
    if not self.isActive:
      return

    self._state = ChannelState.Closed

    if reason == self._CLOSE_INVOKED or self._connection_type == ConnectionRole.Server:
      log_fn = self._log.debug
    else:
      log_fn = self._log.warning
    log_fn('Shutting down transport [%s].' % str(reason))
    self._varz.active(0)
    self._socket.close()
    [g.kill(block=False) for g in self._greenlets]
    self._greenlets = []

    if not isinstance(reason, Exception):
      reason = Exception(str(reason))
    if fault:
      self.on_faulted.Set(reason)
    msg = MethodReturnMessage(error=ClientError(reason))

    for sink_stack, _, _ in self._tag_map.values():
      sink_stack.AsyncProcessResponseMessage(msg)

    if (self._open_result
        and not self._open_result.ready()
        and isinstance(reason, Exception)):
      self._open_result.set_exception(reason)

    self._tag_map = {}
    self._open_result = None
    self._send_queue = Queue()

  @abstractmethod
  def _OnTimeout(self, tag):
    raise NotImplementedError()

  def _HandleTimeout(self, msg_properties):
    """Determine if a message has timed out yet (because it waited in the queue
    for too long).  If it hasn't, initialize the timeout handler to fire if the
    message times out in transit.

    Args:
      msg_properties - The properties of the message.
    """
    timeout_event = msg_properties.get(Deadline.EVENT_KEY, None)
    if timeout_event and timeout_event.Get():
      # The message has timed out before it got out of the send queue
      # In this case, we can discard it immediately without even sending it.
      tag = msg_properties.pop(Tag.KEY, 0)
      if tag != 0:
        self._ReleaseTag(tag)
      return True
    elif timeout_event:
      # The event exists but hasn't been signaled yet, hook up a
      # callback so we can be notified on a timeout.
      def timeout_proc():
        timeout_tag = msg_properties.pop(Tag.KEY, 0)
        if timeout_tag:
          self._OnTimeout(timeout_tag)

      timeout_event.Subscribe(lambda evt: timeout_proc(), True)
      return False
    else:
      # No event was created, so this will never timeout.
      return False

  def _SendLoop(self):
    """Dispatch messages from the send queue to the remote server.

    Note: Messages in the queue have already been serialized into wire format.
    """
    while self.isActive:
      try:
        payload, dct = self._send_queue.get()
        queue_len = self._send_queue.qsize()
        self._varz.send_queue_size(queue_len)
        # HandleTimeout sets up the transport level timeout handling
        # for this message.  If the message times out in transit, this
        # transport will handle sending a Tdiscarded to the server.
        if self._HandleTimeout(dct): continue

        with self._varz.send_time.Measure():
          with self._varz.send_latency.Measure():
            self._socket.write(payload)
        self._varz.messages_sent()
      except Exception as e:
        self._Shutdown(e)
        break

  def _RecvLoop(self):
    """Dispatch messages from the remote server to their recipient.

    Note: Deserialization and dispatch occurs on a seperate greenlet, this only
    reads the message off the wire.
    """
    while self.isActive:
      try:
        sz, = unpack('!i', str(self._socket.readAll(4)))
        with self._varz.recv_time.Measure():
          with self._varz.recv_latency.Measure():
            buf = StringIO(self._socket.readAll(sz))
        self._varz.messages_recv()
        gevent.spawn(self._ProcessRecv, buf)
      except Exception as e:
        self._Shutdown(e)
        break

  @abstractmethod
  def _ProcessRecv(self, stream):
    raise NotImplementedError()

  def _ProcessTaggedReply(self, tag, stream):
    tup = self._ReleaseTag(tag)
    if tup:
      reply_stack, start_time, props = tup
      props[Tag.KEY] = None
      self._varz.transport_latency(time.time() - start_time)
      stream.seek(0)
      reply_stack.AsyncProcessResponseStream(stream)

  def _ReleaseTag(self, tag):
    """Return a tag to the tag pool.

    Note: Tags are only returned when the server has ACK'd them (or NACK'd) with
    and Rdispatch message (or similar).  Client initiated timeouts do NOT return
    tags to the pool.

    Args:
      tag - The tag to return.

    Returns:
      The ClientChannelSinkStack associated with the tag's response.
    """
    tup = self._tag_map.pop(tag, None)
    self._tag_pool.release(tag)
    return tup

  @abstractmethod
  def _BuildHeader(self, tag, msg_type, data_len):
    raise NotImplementedError()

  def _Send(self, data, msg_props):
    self._send_queue.put((data, msg_props))

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    if self._state == ChannelState.Idle and self._open_result:
      self._log.debug('Waiting for channel to be open')
      self._open_result.wait()

    if ((self._state == ChannelState.Idle and not self._open_result)
        or self._state == ChannelState.Closed) and sink_stack is not None:
      err_msg = MethodReturnMessage(error=Exception('Sink not open.'))
      sink_stack.AsyncProcessResponseMessage(err_msg)
      return

    if not msg.is_one_way:
      tag = self._tag_pool.get()
      msg.properties[Tag.KEY] = tag
      self._tag_map[tag] = (sink_stack, time.time(), msg.properties)
    else:
      tag = 0

    data_len = stream.tell()
    header = self._BuildHeader(tag, headers[TransportHeaders.MessageType], data_len)
    payload = header + stream.getvalue()
    self._Send(payload, msg.properties)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    pass
