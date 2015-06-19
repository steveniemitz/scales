import logging

from abc import (
  abstractmethod
)

from ..message import MethodReturnMessage
from ..sink import ClientMessageSink

ROOT_LOG = logging.getLogger("scales")


class PoolSink(ClientMessageSink):
  """A pool maintains a set of zero or more sinks to a single endpoint."""

  def __init__(self, sink_provider, properties):
    self._properties = properties
    self._sink_provider = sink_provider
    super(PoolSink, self).__init__()

  @abstractmethod
  def _Get(self):
    """To be overridden by subclasses.  Called to get a sink from the pool.

    Returns:
      A sink.
    """
    raise NotImplementedError()

  @abstractmethod
  def _Release(self, sink):
    """To be overridden by subclasses.  Called when a sink has completed
    processing.

    Args:
      sink - A sink that had been previously returned from _Get()
    """
    raise NotImplementedError()

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    """Gets a ClientChannelSink from the pool (asynchronously), and continues
    processing the message on it.
    """
    try:
      sink = self._Get()
    except Exception as e:
      ex_msg = MethodReturnMessage(error=e)
      sink_stack.AsyncProcessResponseMessage(ex_msg)
      return

    sink_stack.Push(self, sink)
    sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    """Returns the sink (in 'context') to the pool and delegates to the next sink.
    """
    self._Release(context)
    sink_stack.AsyncProcessResponse(stream, msg)
