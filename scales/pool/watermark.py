from collections import deque
import logging

import gevent

from .base import PoolSink
from ..async import AsyncResult
from ..constants import (Int, ChannelState, SinkProperties)
from ..sink import (
  ClientMessageSink,
  SinkProvider,
  FailingMessageSink
)
from ..dispatch import ServiceClosedError
from ..varz import (
  Gauge,
  SourceType,
  VarzBase
)

class QueuingMessageSink(ClientMessageSink):
  def __init__(self, queue):
    super(QueuingMessageSink, self).__init__()
    self._queue = queue

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    self._queue.append((sink_stack, msg, stream, headers))

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise NotImplementedError("This should never be called")

  @property
  def state(self):
    return ChannelState.Open


class MaxWaitersError(Exception): pass

class WatermarkPoolSink(PoolSink):
  """A watermark pool keeps a cached number of sinks active (the low watermark).
  Once the low watermark is hit, the pool will create new sinks until it hits the
  high watermark.  At that point, it will begin queuing requests.

  The pool guarantees only a single request will be active on any underlying sink
  at any given time, that is, each sink processes requests serially.
  """

  ROOT_LOG = logging.getLogger('scales.pool.WatermarkPool')

  class Varz(VarzBase):
    """
    size - The current size of the pool.
    queue_size - The length of the waiter queue.
    min_size - The configured low-watermark.
    max_size - The configured high-watermark.
    """
    _VARZ_BASE_NAME = 'scales.pool.WatermarkPool'
    _VARZ_SOURCE_TYPE = SourceType.ServiceAndEndpoint
    _VARZ = {
      'size': Gauge,
      'queue_size': Gauge,
      'min_size': Gauge,
      'max_size': Gauge
    }

  def __init__(self, sink_provider, properties):
    endpoint = properties[SinkProperties.Endpoint]
    name = properties[SinkProperties.Service]

    self._cache = deque()
    self._waiters = deque()
    self._min_size = int(properties.get('min_watermark', 1))
    self._max_size = int(properties.get('max_watermark', Int.MaxValue))
    self._max_queue_size = int(properties.get('max_watermark_queue', Int.MaxValue))
    self._current_size = 0
    self._state = ChannelState.Idle
    socket_name = '%s:%s' % (endpoint.host, endpoint.port)
    self._varz = self.Varz((name, socket_name))
    self._log = self.ROOT_LOG.getChild('[%s.%s]' % (name, socket_name))
    self._varz.min_size(self._min_size)
    self._varz.max_size(self._max_size)
    super(WatermarkPoolSink, self).__init__(sink_provider, properties)

  def __PropagateShutdown(self, value):
    self.on_faulted.Set(value)

  def _DiscardSink(self, sink):
    """Close the sink and unsubscribe from fault notifications

    Args:
      sink - The sink to discard.
    """
    sink.on_faulted.Unsubscribe(self.__PropagateShutdown)
    sink.Close()

  def _Dequeue(self):
    """Attempt to get a sink from the cache.

    Returns:
      A sink if one can be taken from the cache, else None.
    """
    while any(self._cache):
      item = self._cache.popleft()
      if item.state <= ChannelState.Open:
        return item
      else:
        self._DiscardSink(item)
    return None

  def _Get(self):
    cached = self._Dequeue()
    if cached:
      return cached
    elif self._current_size < self._max_size:
      self._current_size += 1
      self._varz.size(self._current_size)
      sink = self._sink_provider.CreateSink(self._properties)
      # TODO: we could get a better failure case here by detecting that Open()
      # failed and retrying, however for now the simplest option is to just fail.
      sink.Open().wait()
      sink.on_faulted.Subscribe(self.__PropagateShutdown)
      return sink
    else:
      if len(self._waiters) + 1 > self._max_queue_size:
        return FailingMessageSink(MaxWaitersError())
      else:
        self._varz.queue_size(len(self._waiters) + 1)
        return QueuingMessageSink(self._waiters)

  def _Release(self, sink):
    # Releasing a queuing sink is a noop
    if (isinstance(sink, QueuingMessageSink) or
        isinstance(sink, FailingMessageSink)):
      self._varz.queue_size(len(self._waiters))
      return

    do_close = False
    # This sink is already shutting down
    if self.state == ChannelState.Closed:
      self._current_size -= 1
    # One of the underlying sinks failed, shut down
    elif sink.state == ChannelState.Closed:
      self._current_size -= 1
      self.Close()
    # There are some waiters queued, reuse this sink to process another request.
    elif any(self._waiters):
      gevent.spawn(self._ProcessQueue, sink)
    # We're below the min-size specified, cache this sink
    elif self._current_size <= self._min_size:
      self._cache.append(sink)
    # We're above the min-size, close the sink.
    else:
      self._current_size -= 1
      do_close = True

    self._varz.size(self._current_size)
    if do_close:
      self._DiscardSink(sink)

  def _ProcessQueue(self, sink):
    """Called as a continuation of an underlying sink completing.  Get the
    next waiter and use 'sink' to process it.

    Args:
      sink - An open sink.
    """
    sink_stack, msg, stream, headers = self._waiters.popleft()
    self._varz.queue_size(len(self._waiters))
    # The stack has a QueuingChannelSink on the top now, pop it off
    # and push the real stack back on.
    orig_sink, ctx = sink_stack.Pop()
    sink_stack.Push(orig_sink, sink)
    sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def Open(self):
    ar = AsyncResult()
    ar.SafeLink(self._OpenImpl)
    return ar

  def _OpenImpl(self):
    sink = self._Get()
    self._Release(sink)
    self._state = ChannelState.Open

  def _FlushCache(self):
    [self._DiscardSink(sink) for sink in self._cache]

  def Close(self):
    self._state = ChannelState.Closed
    self._FlushCache()
    fail_sink = FailingMessageSink(ServiceClosedError)
    [fail_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)
     for sink_stack, msg, stream, headers in self._waiters]

  @property
  def state(self):
    return self._state


WatermarkPoolChannelSinkProvider = SinkProvider(WatermarkPoolSink)
