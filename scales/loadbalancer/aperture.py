"""Aperture Load Balancer.
Based on work from finagle's aperture load balancer.
See https://github.com/twitter/finagle/blob/master/finagle-core/src/main/scala/com/twitter/finagle/loadbalancer/Aperture.scala

The aperture balancer attempts to keep the average load going into the underlying
server set between a load band (by default .5 <= load <= 2.

Load is determined via an ema of load over a smoothing window (5 seconds).
The load average is essentially the average number of concurrent requests each
node in the balancer is handling.
"""

import math
import time

from .heap import HeapBalancerSink
from ..async import AsyncResult
from ..constants import (ChannelState, SinkProperties, SinkRole)
from ..sink import SinkProvider
from ..varz import (
  Gauge,
  SourceType,
  VarzBase
)

class MonoClock(object):
  """A clock whose value is guaranteed to always be increasing.
  Clock skew is compensated.
  """
  def __init__(self):
    self._last = time.time()

  def Sample(self):
    """Return the current time, as reported by time.time(), as long as it has
    increased since the last sample."""
    now = time.time()
    if now - self._last > 0:
      self._last = now
    return self._last


class Ema(object):
  """Calculate an exponential moving average over a window."""
  def __init__(self, window):
    """Args:
      window - The smoothing window, in seconds, to calculate the EMA over.
    """
    self._window = window
    self._time = -1
    self._ema = 0.0

  def Update(self, ts, sample):
    """Update the EMA with a new sample
    Args:
      ts - The timestamp, in seconds.
      sample - The sampled value.
    Returns:
      The current EMA after being updated with the sample.
    """
    if self._time == -1:
      self._time = ts
      self._ema = float(sample)
    else:
      delta = ts - self._time
      self._time = ts
      window = 0 if self._window == 0 else math.exp(-float(delta) / self._window)
      self._ema = (sample * (1-window)) + (self._ema * window)
    return self._ema


class ApertureBalancerSink(HeapBalancerSink):
  """A load balancer that keeps an aperture adjusted by a load average."""

  class ApertureVarz(VarzBase):
    """
    idle - The number of nodes idle in the pool (not in the aperture)
    active - The number of nodes active in the pool (in the aperture)
    load_average - The most recently calculated load average.
    """
    _VARZ_BASE_NAME = 'scales.pool.ApertureBalancer'
    _VARZ_SOURCE_TYPE = SourceType.Service
    _VARZ = {
      'idle': Gauge,
      'active': Gauge,
      'load_average': Gauge
    }

  def __init__(self, next_provider, sink_properties, global_properties):
    self._idle_sinks = set()
    self._active_sinks = set()
    self._total = 0
    self._ema = Ema(5)
    self._time = MonoClock()
    self._min_size = sink_properties.min_size
    self._min_load = sink_properties.min_load
    self._max_load = sink_properties.max_load
    service_name = global_properties[SinkProperties.Label]
    self.__varz = self.ApertureVarz(service_name)
    super(ApertureBalancerSink, self).__init__(next_provider, sink_properties, global_properties)

  def _UpdateSizeVarz(self):
    """Update active and idle varz"""
    self.__varz.active(len(self._active_sinks))
    self.__varz.idle(len(self._idle_sinks))

  def _AddNode(self, sink):
    """Invoked when a node is added to the underlying server set.

    If the number of healthy nodes is < the minimum aperture size, the node
    will be added to the aperture, otherwise it will be added to the idle channel
    list.

    Args:
      sink - The sink created by the load balancer.
    """
    num_healthy = len([c for c in self._active_sinks if c.state <= ChannelState.Open])
    if num_healthy < self._min_size:
      self._active_sinks.add(sink)
      super(ApertureBalancerSink, self)._AddNode(sink)
    else:
      self._idle_sinks.add(sink)
    self._UpdateSizeVarz()

  def _RemoveNode(self, sink):
    """Invoked when a node is removed from the underlying server set.

    If the node is currently active, it is removed from the aperture and replaced
    by an idle node (if one is available).  Otherwise, it is simply discarded.

    Args:
      sink - The sink being removed from the server set.
    """
    super(ApertureBalancerSink, self)._RemoveNode(sink)
    if sink in self._active_sinks:
      self._active_sinks.discard(sink)
      self._TryExpandAperture()
    if sink in self._idle_sinks:
      self._idle_sinks.discard(sink)
    self._UpdateSizeVarz()

  def _TryExpandAperture(self):
    """Attempt to expand the aperture.  By calling this it's assumed the aperture
    needs to be expanded.

    The aperture can be expanded if there are idle sinks available.
    """
    sinks = self._idle_sinks.copy()
    added_node = None
    while any(sinks):
      new_sink = sinks.pop()
      if new_sink.state != ChannelState.Closed:
        self._idle_sinks.discard(new_sink)
        self._active_sinks.add(new_sink)
        self._log.debug('Expanding aperture.')
        added_node = super(ApertureBalancerSink, self)._AddNode(new_sink)
        break

    self._UpdateSizeVarz()
    if added_node:
      return added_node
    else:
      return AsyncResult.Complete()

  def _ContractAperture(self):
    """Attempt to contract the aperture.  By calling this it's assume the aperture
    needs to be contracted.

    The aperture can be contracted if it's current size is larger than the
    min size.
    """
    if len(self._active_sinks) > self._min_size:
      heap_range = len(self._heap) / 2
      most_loaded = None
      # Scan the right half of the heap for the least-loaded node.
      for n in self._heap[heap_range:]:
        if not most_loaded or n.load > most_loaded.load:
          most_loaded = n
      most_loaded_sink = most_loaded.channel
      self._active_sinks.discard(most_loaded_sink)
      self._idle_sinks.add(most_loaded_sink)
      self._log.debug('Contracting aperture to remove %s' % most_loaded_sink)
      super(ApertureBalancerSink, self)._RemoveNode(most_loaded_sink)
      self._UpdateSizeVarz()

  def _OnNodeDown(self, node):
    """Invoked by the base class when a node is marked down.
    In this case, if the downed node is currently in the aperture, we want to
    remove if, and then attempt to adjust the aperture.
    """
    if node.channel in self._active_sinks:
      return self._TryExpandAperture()
    else:
      return AsyncResult.Complete()

  def _OnGet(self, node):
    """Invoked by the parent class when a node has been retrieved from the pool
    and is about to be used.
    Increases the load average of the pool, and adjust the aperture if needed.
    """
    self._AdjustAperture(1)

  def _OnPut(self, node):
    """Invoked by the parent class when a node is being returned to the pool.
    Decreases the load average and adjust the aperture if needed.
    """
    self._AdjustAperture(-1)

  def _AdjustAperture(self, amount):
    """Adjusts the load average of the pool, and adjusts the aperture size
    if required by the new load average.

    Args:
      amount - The amount to change the load by.  May be +/-1
    """
    self._total += amount
    avg = self._ema.Update(self._time.Sample(), self._total)
    aperture_size = len(self._active_sinks)
    if aperture_size == 0:
      # Essentially infinite load.
      aperture_load = self._max_load
    else:
      aperture_load = avg / aperture_size
      self.__varz.load_average(aperture_load)
    if aperture_load >= self._max_load and any(self._idle_sinks):
      self._TryExpandAperture()
    elif aperture_load <= self._min_load and aperture_size > self._min_size:
      self._ContractAperture()

ApertureBalancerSink.Builder = SinkProvider(
  ApertureBalancerSink,
  SinkRole.LoadBalancer,
  smoothing_window = 5,
  min_size = 1,
  min_load = 0.5,
  max_load = 2.0,
  server_set_provider = None)
