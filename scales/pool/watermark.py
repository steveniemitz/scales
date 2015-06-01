from collections import deque
import logging

import gevent

from .base import PoolChannelSink
from ..constants import (Int, ChannelState)
from ..sink import (
  ClientChannelSink,
  ChannelSinkProvider,
  FailingChannelSink
)
from ..dispatch import ServiceClosedError
from ..varz import (
  Counter,
  Gauge,
  SourceType,
  VarzBase
)

class QueuingChannelSink(ClientChannelSink):
  def __init__(self, queue):
    super(QueuingChannelSink, self).__init__()
    self._queue = queue

  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    self._queue.append((sink_stack, msg, stream, headers))

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    raise NotImplementedError("This should never be called")

  def Open(self): pass
  def Close(self): pass
  @property
  def state(self): pass

class WatermarkPoolChannelSink(PoolChannelSink):
  ROOT_LOG = logging.getLogger('scales.pool.WatermarkPool')

  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.pool.WatermarkPool'
    _VARZ_SOURCE_TYPE = SourceType.ServiceAndEndpoint
    _VARZ = {
      'size': Gauge,
      'queue_size': Gauge,
      'min_size': Gauge,
      'max_size': Gauge
    }

  def __init__(self, sink_provider, endpoint, name, properties):
    self._cache = deque()
    self._waiters = deque()
    self._min_size = 1
    self._max_size = Int.MaxValue
    self._current_size = 0
    self._state = ChannelState.Open
    socket_name = '%s:%s' % (endpoint.host, endpoint.port)
    self._varz = self.Varz((name, socket_name))
    self._log = self.ROOT_LOG.getChild('[%s.%s]' % (name, socket_name))
    self._varz.min_size(self._min_size)
    self._varz.max_size(self._max_size)
    super(WatermarkPoolChannelSink, self).__init__(sink_provider, endpoint, name, properties)

  def _Dequeue(self):
    while any(self._cache):
      item = self._cache.popleft()
      if item.state <= ChannelState.Open:
        return item
    return None

  def _Get(self):
    cached = self._Dequeue()
    if cached:
      return cached
    elif self._current_size < self._max_size:
      self._current_size += 1
      self._varz.size(self._current_size)
      sink = self._sink_provider.CreateSink(self._endpoint, self._name, None)
      sink.Open()
      return sink
    else:
      self._varz.queue_size(len(self._waiters) + 1)
      return QueuingChannelSink(self._waiters)

  def _Release(self, sink):
    if isinstance(sink, QueuingChannelSink):
      return

    do_close = False
    if self.state == ChannelState.Closed:
      self._current_size -= 1
    elif sink.state == ChannelState.Closed:
      self._current_size -= 1
      self.Close()
    elif any(self._waiters):
      # Reuse this sink to process another message
      gevent.spawn(self._ProcessQueue, sink)
    elif self._current_size <= self._min_size:
      self._cache.append(sink)
    else:
      self._current_size -= 1
      do_close = True

    self._varz.size(self._current_size)
    if do_close:
      sink.Close()

  def _ProcessQueue(self, sink):
    sink_stack, msg, stream, headers = self._waiters.popleft()
    self._varz.queue_size(len(self._waiters))
    # The stack has a QueuingChannelSink on the top now, pop it off
    # and push the real stack back on.
    orig_sink, ctx = sink_stack.Pop()
    sink_stack.Push(orig_sink, sink)
    sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def Open(self):
    pass

  def Close(self):
    self._state = ChannelState.Closed
    fail_sink = FailingChannelSink(ServiceClosedError)

    [sink.Close() for sink in self._cache]
    [fail_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)
     for sink_stack, msg, stream, headers in self._waiters]
    self._on_faulted.Set(None)

  @property
  def state(self):
    return self._state


WatermarkPoolChannelSinkProvider = ChannelSinkProvider(WatermarkPoolChannelSink)
