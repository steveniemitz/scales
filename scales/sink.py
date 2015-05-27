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
)
from collections import deque

import gevent
from gevent.event import AsyncResult

from .constants import DispatcherState
from .message import (
  MethodReturnMessage,
  ScalesClientError
)


class MessageSink(object):
  """A base class for all message sinks.

  MessageSinks form a cooperative linked list, which each sink calling the
  next sink in the chain once it's processing is complete.
  """
  __metaclass__ = ABCMeta

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


class ReplySink(MessageSink):
  """ReplySinks are MessageSinks for processing the asynchronous return message
  from a AsyncProcessRequest."""
  def __init__(self):
    super(ReplySink, self).__init__()

  @abstractmethod
  def ProcessReturnMessage(self, msg):
    """Performs processing on msg.

    Implementors should then call next_sink.ProcessReturnMessage(msg).

    Args:
      msg - The message to process.
    """
    raise NotImplementedError()


class AsyncMessageSink(MessageSink):
  """AsyncMessageSinks take a message, perform processing, and forward it to the
  next sink in the chain (next_sink).
  """
  def __init__(self):
    super(AsyncMessageSink, self).__init__()

  @abstractmethod
  def AsyncProcessMessage(self, msg, reply_sink):
    """Perform processing on a message.

    Args:
      msg - The message to process.
      reply_sink - A ReplySink that will receive the response message.
    """
    raise NotImplementedError()


class ClientChannelSink(MessageSink):
  """ClientChannelSinks take a message, stream, and headers and perform
  processing on them.
  """
  def __init__(self):
    super(ClientChannelSink, self).__init__()

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
  def AsyncProcessResponse(self, sink_stack, context, stream):
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


class ClientChannelTransportSink(ClientChannelSink):
  """ClientChannelTransportSinks represent the last sink in the chain.

  They are responsible for sending the serialized message to a remote server and
  receiving the response.
  """

  def __init__(self):
    super(ClientChannelTransportSink, self).__init__()
    self._shutdown_result = AsyncResult()
    self._state = DispatcherState.UNINITIALIZED

  @property
  def shutdown_result(self):
    """An AsyncResult representing the state of the sink.  This result is
    signaled when the sink shuts down.
    """
    return self._shutdown_result

  def _Shutdown(self, reason):
    """Shutdown the sink and signal.

    Args:
      reason - The reason the shutdown occurred.  May be an exception or string.
    """
    if not self.isOpen():
      return False

    self._ShutdownImpl()
    if not isinstance(reason, Exception):
      reason = Exception(str(reason))
    self._shutdown_result.set_exception(reason)
    return True

  @abstractmethod
  def _ShutdownImpl(self):
    """To be implemented by subclasses.  Intended to perform any teardown needed
    during shutdown (closing sockets, etc).
    """
    pass

  @abstractmethod
  def _Open(self):
    """To be implemented by subclasses.  Intended to perform any initialization
    needed to allow the channel to communicate with a remote server.
    """
    pass

  @abstractmethod
  def isOpen(self):
    """Returns True if the channel has been opened, else False."""
    pass

  def open(self):
    """Opens the channel, initializing any resources needed to communicate
    with a remote server."""
    self._Open()

  @abstractmethod
  def testConnection(self):
    """Test the underlying resource

    Returns:
      True if the underlying resource is healthy, else False.
    """
    pass

  def close(self):
    """Close the underlying resource and shut down this channel."""
    self._Shutdown('Close Invoked')

  def AsyncProcessResponse(self, sink_stack, context, stream):
    """AsyncProcessResponse should never be called on ClientChannelTransportSinks,
    as they are the sink responsible for handling the response.
    """
    raise Exception("This should never be called.")


class ClientFormatterSink(AsyncMessageSink, ClientChannelSink):
  """ClientFormatterSinks bridge a AsyncMessageSink and ClientChannelSink.

  They are the final AsyncMessageSink in the message sink chain, and the first
  ClientChannelSink in the channel sink chain.  Therefor, they take a message,
  serialize it to a wire format, then hand it off to the channel sink chain.
  """
  def __init__(self):
    super(ClientFormatterSink, self).__init__()

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    """Since ClientFormatterSinks are the first sink in the client channel sink
    chain, they should never have AsyncProcessRequest called on them."""
    raise Exception('This should never be called.')


class SinkStack(object):
  """A stack of sinks."""
  def __init__(self):
    self._stack = deque()

  def Push(self, sink, context=None):
    """Push a sink, and optional context data, onto the stack.

    Args:
      sink - The sink to push onto the stack.
      context - Optional context data associated with the current processing
                state of the sink.
    """
    self._stack.append((sink, context))

  def Pop(self):
    return self._stack.pop()


class MessageSinkStackBuilder(object):
  """A factory class responsible for creating a sink chain.
  """
  __metaclass__ = ABCMeta

  @abstractmethod
  def CreateSinkStack(self, name):
    """Create set of message sinks.

    Args:
      name - The name of the service requesting the sinks.
    Returns:
      The head of the message sink chain.
    """
    raise NotImplementedError()


class TransportSinkStackBuilder(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def CreateSinkStack(self, server, name):
    pass

  @abstractmethod
  def AreTransportsSharable(self):
    pass

  @abstractmethod
  def IsConnectionFault(self, e):
    pass

class ClientChannelSinkStack(SinkStack):
  """A SinkStack of ClientChannelSinks.

  The ClientChannelSinkStack add forwards AsyncProcessResponse to the next sink
  on the stack, or DispatchReplyMessage to the reply sink.
  """
  def __init__(self, reply_sink):
    """
    Args:
      reply_sink - An optional ReplySink.
    """
    super(ClientChannelSinkStack, self).__init__()
    self._reply_sink = reply_sink

  def AsyncProcessResponse(self, stream):
    """Pops the next sink off the stack and calls AsyncProcessResponse on it.

    Args:
      stream - The stream to pass to the next sink.
    """
    self._PopProcessResponse(stream)

  def DispatchReplyMessage(self, msg):
    """If a reply sink was supplied, calls ProcessReturnMessage on it.

    Args:
      msg - The message to dispatch.
    """
    if self._reply_sink:
      self._reply_sink.ProcessReturnMessage(msg)

  def _PopProcessResponse(self, stream):
    next_sink, next_ctx = self.Pop()
    next_sink.AsyncProcessResponse(self, next_ctx, stream)

  @property
  def is_one_way(self):
    return self._reply_sink is None


class PooledTransportSink(ClientChannelSink):
  """A ClientChannelSink that delegates processing to a ClientChannelSink
   acquired from a pool.
  """
  def __init__(self, pool):
    """
    Args:
      pool - The pool of ClientChannelSinks to use.
    """
    super(PooledTransportSink, self).__init__()
    self._pool = pool

  def _AsyncProcessRequestCallback(self, sink_stack, msg, stream, headers):
    """Continues processing of AsyncProcessRequest"""
    try:
      shard, sink = self._pool.Get()
    except Exception as e:
      excr = MethodReturnMessage(error=e)
      sink_stack.DispatchReplyMessage(excr)
      return

    sink_stack.Push(self, (shard, sink))
    sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    """Gets a ClientChannelSink from the pool (asynchronously), and continues
    the sink chain on it.
    """

    # Getting a member of the pool may involve a blocking operation.  In order
    # to maintain a fully asynchronous stack, the rest of the chain is executed
    # on a worker greenlet.
    gevent.spawn(self._AsyncProcessRequestCallback, sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream):
    """Returns the ClientChannelSink to the pool and delegates to the next sink.

    Args:
      sink_stack - The sink stack.
      context - A tuple supplied from AsyncProcessRequest of (shard, channel sink).
      stream - The stream being processed.
    """
    self._pool.Return(*context)
    sink_stack.AsyncProcessResponse(stream)
