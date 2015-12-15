"""Sinks are classes that control and modify the flow of messages through the
 RPC system.

 Sinks cooperatively chain together in a linked list.  Each sink in the chain
 calls the next sink until the chain terminates.  On the response side, sinks
 cooperatively propagate the response to the next via a sink stack.
"""

from abc import (
  ABCMeta,
  abstractmethod,
  abstractproperty
)
from collections import (deque, namedtuple)
from weakref import WeakValueDictionary
import time

from gevent.coros import RLock

from .async import AsyncResult
from .constants import (ChannelState, SinkProperties, SinkRole)
from .observable import Observable
from .message import (
  Deadline,
  MethodReturnMessage,
  TimeoutError
)
from .scales_socket import ScalesSocket
from .timer_queue import GLOBAL_TIMER_QUEUE
from .varz import (
  Rate,
  Source,
  VarzBase,
  VarzSocketWrapper
)

class MessageSink(object):
  """A base class for all message sinks.

  MessageSinks form a cooperative linked list, which each sink calling the
  next sink in the chain once it's processing is complete.
  """
  __metaclass__ = ABCMeta
  __slots__ = '_next',

  def __init__(self):
    super(MessageSink, self).__init__()
    self._next = None

  @property
  def next_sink(self):
    """The next sink in the chain."""
    return self._next

  @next_sink.setter
  def next_sink(self, value):
    self._next = value


class ClientMessageSink(MessageSink):
  """ClientMessageSinks take a message, stream, and headers and perform
  processing on them.
  """
  __slots__ = '_on_faulted',
  Role = None
  Builder = None

  def __init__(self):
    self._on_faulted = Observable()
    super(ClientMessageSink, self).__init__()

  @property
  def state(self):
    return self.next_sink.state

  @property
  def is_open(self):
    """Returns True if the sink is Idle, Open, or Busy"""
    return self.state <= ChannelState.Busy

  @property
  def is_closed(self):
    """Returns True if the sink is Closed."""
    return self.state == ChannelState.Closed

  @property
  def is_ready(self):
    """Returns True if the channel is open, eg ready to process messages."""
    return self.state == ChannelState.Open

  @property
  def on_faulted(self):
    return self._on_faulted

  def Open(self):
    if self.next_sink:
      return self.next_sink.Open()
    else:
      return AsyncResult.Complete()

  def Close(self):
    if self.next_sink:
      self.next_sink.Close()

  @abstractmethod
  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    """Process a request message, stream, and headers.

    Args:
      sink_stack - The SinkStack representing the processing state of the message.
                   Implementors should push their sink onto this stack before
                   forwarding the message in order to participate in processing
                   the response.
      msg - The message being processed.
      stream - A serialized version of the message.
      headers - Any additional headers to be sent.
    """
    raise NotImplementedError()

  @abstractmethod
  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    """Process a response stream.

    Args:
      sink_stack - The SinkStack representing the processing state of the message.
                   Implementors should call sink_stack.AsyncProcessMessage(...)
                   to forward the message to the next sink.
      context - The context that was pushed onto the stack in AsyncProcessRequest.
      stream - The stream representing the serialized response.
    """
    raise NotImplementedError()


class ServerMessageSink(ClientMessageSink):
  pass

class ServerChannelSink(object):
  pass

class SinkStack(object):
  """A stack of sinks."""
  __slots__ = '_stack',

  def __init__(self):
    self._stack = deque()

  def Push(self, sink, context=None):
    """Push a sink, and optional context data, onto the stack.

    Args:
      sink - The sink to push onto the stack.
      context - Optional context data associated with the current processing
                state of the sink.
    """
    if sink is None:
      raise Exception("sink must not be None")

    self._stack.append((sink, context))

  def Pop(self):
    return self._stack.pop()

  def Any(self):
    return any(self._stack)


class ClientMessageSinkStack(SinkStack):
  """A SinkStack of ClientMessageSinks.

  The ClientMessageSinkStack forwards AsyncProcessResponse to the next sink
  on the stack.
  """

  def __init__(self):
    """
    Args:
      reply_sink - An optional ReplySink.
    """
    super(ClientMessageSinkStack, self).__init__()

  def AsyncProcessResponse(self, stream, msg):
    """Pop the next sink off the stack and call AsyncProcessResponse on it."""
    if self.Any():
      next_sink, next_ctx = self.Pop()
      next_sink.AsyncProcessResponse(self, next_ctx, stream, msg)

  def AsyncProcessResponseStream(self, stream):
    self.AsyncProcessResponse(stream, None)

  def AsyncProcessResponseMessage(self, msg):
    self.AsyncProcessResponse(None, msg)


class FailingMessageSink(ClientMessageSink):
  """A sink that always returns a failure message."""

  def __init__(self, ex):
    self._ex = ex
    super(FailingMessageSink, self).__init__()

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    msg = MethodReturnMessage(error=self._ex())
    sink_stack.AsyncProcessResponseMessage(msg)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise NotImplementedError("This should never be called")

  @property
  def state(self):
    return ChannelState.Open

  @property
  def endpoint(self):
    return None

class ClientTimeoutSink(ClientMessageSink):
  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.TimeoutSink'
    _VARZ = {
      'timeouts': Rate
    }

  def __init__(self, next_provider, sink_properties, global_properties):
    super(ClientTimeoutSink, self).__init__()
    self.next_sink = next_provider.CreateSink(global_properties)
    self._varz = self.Varz(
      Source(service=global_properties[SinkProperties.Label]))

  def _TimeoutHelper(self, evt, sink_stack):
    """Waits for ar to be signaled or [timeout] seconds to elapse.  If the
    timeout elapses, the event on the message will be signaled, and a timeout
    message posted to the sink_stack, aborting the message call.
    """
    if evt:
      evt.Set(True)
    self._varz.timeouts()
    error_msg = MethodReturnMessage(error=TimeoutError())
    sink_stack.AsyncProcessResponseMessage(error_msg)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    """Initialize the timeout handler for this request.

    Args:
      ar - The AsyncResult for the pending response of this request.
      timeout - An optional timeout.  If None, no timeout handler is initialized.
      tag - The tag of the request.
    """
    deadline = msg.properties.get(Deadline.KEY)
    if deadline:
      now = time.time()
      if deadline < now:
        self._TimeoutHelper(None, sink_stack)
        return

      evt = Observable()
      msg.properties[Deadline.EVENT_KEY] = evt
      cancel_timeout = GLOBAL_TIMER_QUEUE.Schedule(
        deadline, lambda: self._TimeoutHelper(evt, sink_stack))
      sink_stack.Push(self, cancel_timeout)
    return self.next_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    context()
    sink_stack.AsyncProcessResponse(stream, msg)


class SinkProviderBase(object):
  """Base class for sink providers."""
  __metaclass__ = ABCMeta
  __slots__ = 'next_provider', 'sink_properties'
  _defaults = {}
  SINK_CLASS = None
  PARAMS_CLASS = None
  Role = None

  def __init__(self, **kwargs):
    new_props = self._defaults.copy()
    new_props.update(kwargs)
    if self.PARAMS_CLASS:
      if not callable(self.PARAMS_CLASS):
        raise Exception('PARAMS_CLASS must be callable')
      self.sink_properties = self.PARAMS_CLASS(**new_props) # pylint: disable=E1102
    else:
      self.sink_properties = None
    self.next_provider = None

  @abstractmethod
  def CreateSink(self, properties):
    pass

  @abstractproperty
  def sink_class(self):
    pass

  def Clone(self, **kwargs):
    """Creates a copy of this sink_provider with new properties"""
    new_props = self.sink_properties.__dict__.copy()
    new_props.update(kwargs)
    new_provider = type(self)(**new_props)
    new_provider.next_provider = self.next_provider
    return new_provider


def SinkProvider(sink_cls, role=None, **defaults):
  """Factory for creating simple sink providers.

  Args:
    sink_cls - The type of sink to provide.
  Returns:
    A SinkProvider that provides sinks of type 'sink_cls'.
  """
  field_names = ' '.join(defaults.keys())
  params_cls = namedtuple('Params', field_names)

  def CreateSink(self, properties):
    return self.SINK_CLASS(self.next_provider, self.sink_properties, properties)

  def sink_class(self):
    return self.SINK_CLASS

  provider = type(
    sink_cls.__name__ + 'Provider',
    (SinkProviderBase, ),
    {
      'SINK_CLASS': sink_cls,
      'PARAMS_CLASS': params_cls,
      'Role': role,
      'CreateSink': CreateSink,
      'sink_class': property(sink_class),
      '_defaults': defaults
    }
  )
  return provider

TimeoutSinkProvider = SinkProvider(ClientTimeoutSink)

def SocketTransportSinkProvider(sink_cls):
  class _SocketTransportSinkProvider(SinkProviderBase):
    SINK_CLS = sink_cls
    Role = SinkRole.Transport

    def CreateSink(self, properties):
      server = properties[SinkProperties.Endpoint]
      service = properties[SinkProperties.Label]
      sock = ScalesSocket(server.host, server.port)
      healthy_sock = VarzSocketWrapper(sock, service)
      sink = self.SINK_CLS(healthy_sock, service)
      return sink

    @property
    def sink_class(self):
      return self.SINK_CLS

  return _SocketTransportSinkProvider

class RefCountedSink(ClientMessageSink):
  def __init__(self, next_sink):
    super(RefCountedSink, self).__init__()
    self._ref_count = 0
    self._open_lock = RLock()
    self._open_ar = None
    self.next_sink = next_sink

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    self.next_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise NotImplementedError("Not called")

  @property
  def on_faulted(self):
    return self.next_sink.on_faulted

  def Open(self):
    with self._open_lock:
      self._ref_count += 1
      if self._ref_count == 1:
        self._open_ar = self.next_sink.Open()
    return self._open_ar

  def Close(self):
    with self._open_lock:
      if self._ref_count == 0:
        return
      self._ref_count -= 1
      if self._ref_count == 0:
        self._open_ar = None
        self.next_sink.Close()

class SharedSinkProvider(SinkProviderBase):
  def __init__(self, key_selector):
    self._key_selector = key_selector
    self._cache = WeakValueDictionary()
    super(SharedSinkProvider, self).__init__()

  def CreateSink(self, properties):
    key = self._key_selector(properties)
    if key:
      sink = self._cache.get(key)
      if not sink:
        new_sink = self.next_provider.CreateSink(properties)
        sink = RefCountedSink(new_sink)
        self._cache[key] = sink
      return sink
    else:
      return self.next_provider.CreateSink(properties)

  @property
  def sink_class(self):
    return self.next_provider.sink_class
