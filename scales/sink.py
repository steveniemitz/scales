"""Sinks are classes that control and modify the flow of messages through the
 RPC system.

 Sinks cooperatively chain together in a linked list to form a _sink stack_.
 Each sink in the chain calls the next sink until the chain terminates.

 Scales has three types of sinks, both derived from MessageSink.
  - AsyncMessageSink:
      AsyncMessageSink handles dispatching a message down the sink chain.
      The chain is initiated by calling AsyncProcessMessage on the head.

  - ReplySink:
      ReplySink handles the message return path.  AsyncMessageSinks may optionally
      add their own ReplySink to the reply sink stack.

  - ClientChannelSink:
      ClientChannelSinks operate on a serialized data stream, representing the
      message.  Much like AsyncMessageSinks, they perform work, then delegate to
      the next sink until the chain terminates.

  Within ClientChannelSinks, there are also two specialized types:
    - ClientFormatterSink:
        A ClientFormatterSink acts as both a ClientChannelSink and AsyncMessageSink.
        It bridges the two, terminating the AsyncMessageSink chain and initiating the
        ClientChannelSink chain.  To do this, it serializes the message to a
        stream (in an implementation specific wire format), and calls its next
        sink as a ClientChannelSink.

    - ClientChannelTransportSink:
        ClientChannelTransportSinks act as the terminating sink of a sink chain.
        They take a serialized stream and handle transporting it to the downstream
        server, as well as handling the response.  Because of the fully asynchronous
        nature of the sink stack, the transport sink also is responsible for
        correlating requests to responses.
"""
from abc import (
  ABCMeta,
  abstractmethod,
  abstractproperty
)
from collections import deque

from . import async_util
from .constants import (ChannelState, SinkProperties)
from .observable import Observable
from .message import (
  Deadline,
  MethodCallMessage,
  MethodReturnMessage,
  TimeoutError
)
from .timer_queue import GLOBAL_TIMER_QUEUE
from .varz import (
  Counter,
  VarzBase
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
  """ClientChannelSinks take a message, stream, and headers and perform
  processing on them.
  """
  __slots__ = '_on_faulted',

  def __init__(self):
    self._on_faulted = Observable()
    super(ClientMessageSink, self).__init__()

  @property
  def state(self):
    return self.next_sink.state

  @property
  def on_faulted(self):
    return self._on_faulted

  def Open(self, force=False):
    if self.next_sink:
      return self.next_sink.Open()
    else:
      return async_util.Complete()

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
                   to forward the message to the next ChannelSink, or
                   sink_stack.DispatchReplyMessage to begin calling reply sinks.
      context - The context that was pushed onto the stack in AsyncProcessRequest.
      stream - The stream representing the serialized response.
    """
    raise NotImplementedError()


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
  """A SinkStack of ClientChannelSinks.

  The ClientChannelSinkStack add forwards AsyncProcessResponse to the next sink
  on the stack, or DispatchReplyMessage to the reply sink.
  """

  def __init__(self):
    """
    Args:
      reply_sink - An optional ReplySink.
    """
    super(ClientMessageSinkStack, self).__init__()

  def AsyncProcessResponse(self, stream, msg):
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


class ClientTimeoutSink(ClientMessageSink):
  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.TimeoutSink'
    _VARZ = {
      'timeouts': Counter
    }

  def __init__(self, next_provider, properties):
    super(ClientTimeoutSink, self).__init__()
    self.next_sink = next_provider.CreateSink(properties)
    self._varz = self.Varz(properties[SinkProperties.Service])

  def _TimeoutHelper(self, evt, sink_stack):
    """Waits for ar to be signaled or [timeout] seconds to elapse.  If the
    timeout elapses, a Tdiscarded message will be queued to the server indicating
    the client is no longer expecting a reply.
    """
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
    if deadline and isinstance(msg, MethodCallMessage):
      evt = Observable()
      msg.properties[Deadline.EVENT_KEY] = evt
      cancel_timeout = GLOBAL_TIMER_QUEUE.Schedule(deadline, lambda: self._TimeoutHelper(evt, sink_stack))
      sink_stack.Push(self, cancel_timeout)
    return self.next_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    context()
    sink_stack.AsyncProcessResponse(stream, msg)


class ChannelSinkProviderBase(object):
  __metaclass__ = ABCMeta

  def __init__(self):
    self.next_provider = None

  @abstractmethod
  def CreateSink(self, properties):
    pass

  @abstractproperty
  def sink_class(self):
    pass

def ChannelSinkProvider(sink_cls):
  class _ChannelSinkProvider(ChannelSinkProviderBase):
    __slots__ = 'next_provider',
    __metaclass__ = ABCMeta
    SINK_CLASS = sink_cls

    def CreateSink(self, properties):
      return self.SINK_CLASS(self.next_provider, properties)

    @property
    def sink_class(self):
      return self.SINK_CLASS
  return _ChannelSinkProvider


TimeoutSinkProvider = ChannelSinkProvider(ClientTimeoutSink)
