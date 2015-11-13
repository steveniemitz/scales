"""Aperture Load Balancer.
Based on work from finagle's aperture load balancer.
See https://github.com/twitter/finagle/blob/master/finagle-core/src/main/scala/com/twitter/finagle/loadbalancer/Aperture.scala

The aperture balancer attempts to keep the average load going into the underlying
server set between a load band (by default .5 <= load <= 2.

Load is determined via an ema of load over a smoothing window (5 seconds).
The load average is essentially the average number of concurrent requests each
node in the balancer is handling.
"""

import random

from .heap import HeapBalancerSink
from ..async import AsyncResult
from ..constants import (ChannelState, SinkProperties, SinkRole)
from ..sink import SinkProvider
from ..timer_queue import LOW_RESOLUTION_TIMER_QUEUE, LOW_RESOLUTION_TIME_SOURCE
from ..varz import (
  Ema,
  Gauge,
  MonoClock,
  Source,
  VarzBase
)


class ApertureBalancerSink(HeapBalancerSink):
  """A load balancer that keeps an aperture adjusted by a load average."""

  class ApertureVarz(VarzBase):
    """
    idle - The number of nodes idle in the pool (not in the aperture)
    active - The number of nodes active in the pool (in the aperture)
    load_average - The most recently calculated load average.
    """
    _VARZ_BASE_NAME = 'scales.loadbalancer.Aperture'
    _VARZ = {
      'idle': Gauge,
      'active': Gauge,
      'load_average': Gauge
    }

  def __init__(self, next_provider, sink_properties, global_properties):
    self._idle_endpoints = set()
    self._total = 0
    self._ema = Ema(5)
    self._time = MonoClock()
    self._min_size = sink_properties.min_size
    self._max_size = sink_properties.max_size
    self._min_load = sink_properties.min_load
    self._max_load = sink_properties.max_load
    self._jitter_min = sink_properties.jitter_min_sec
    self._jitter_max = sink_properties.jitter_max_sec
    service_name = global_properties[SinkProperties.Label]
    self.__varz = self.ApertureVarz(Source(service=service_name))
    self._pending_endpoints = set()
    super(ApertureBalancerSink, self).__init__(next_provider, sink_properties, global_properties)
    if self._jitter_min > 0:
      self._ScheduleNextJitter()

  def _UpdateSizeVarz(self):
    """Update active and idle varz"""
    self.__varz.active(self._size)
    self.__varz.idle(len(self._idle_endpoints))

  def _AddSink(self, endpoint, sink_factory):
    """Invoked when a node is added to the underlying server set.

    If the number of healthy nodes is < the minimum aperture size, the node
    will be added to the aperture, otherwise it will be added to the idle channel
    list.

    Args:
      endpoint - The endpoint being added to the server set.
      sink_factory - A callable used to create a sink for the endpoint.
    """
    num_healthy = len([c for c in self._heap[1:] if c.channel.is_open])
    if num_healthy < self._min_size:
      super(ApertureBalancerSink, self)._AddSink(endpoint, sink_factory)
    else:
      self._idle_endpoints.add(endpoint)
    self._UpdateSizeVarz()

  def _RemoveSink(self, endpoint):
    """Invoked when a node is removed from the underlying server set.

    If the node is currently active, it is removed from the aperture and replaced
    by an idle node (if one is available).  Otherwise, it is simply discarded.

    Args:
      endpoint - The endpoint being removed from the server set.
    """
    removed = super(ApertureBalancerSink, self)._RemoveSink(endpoint)
    if removed:
      self._TryExpandAperture()
    if endpoint in self._idle_endpoints:
      self._idle_endpoints.discard(endpoint)
    self._UpdateSizeVarz()

  def _TryExpandAperture(self, leave_pending=False):
    """Attempt to expand the aperture.  By calling this it's assumed the aperture
    needs to be expanded.

    The aperture can be expanded if there are idle sinks available.
    """
    endpoints = list(self._idle_endpoints)
    added_node = None
    new_endpoint = None
    if endpoints:
      new_endpoint = random.choice(endpoints)
      self._idle_endpoints.discard(new_endpoint)
      self._log.debug('Expanding aperture to include %s.' % str(new_endpoint))
      new_sink = self._servers[new_endpoint]
      self._pending_endpoints.add(new_endpoint)
      added_node = super(ApertureBalancerSink, self)._AddSink(new_endpoint, new_sink)

    self._UpdateSizeVarz()
    if added_node:
      if not leave_pending:
        added_node.ContinueWith(
            lambda ar: self._pending_endpoints.discard(new_endpoint))
      return added_node, new_endpoint
    else:
      return AsyncResult.Complete(), None

  def _ContractAperture(self, force=False):
    """Attempt to contract the aperture.  By calling this it's assume the aperture
    needs to be contracted.

    The aperture can be contracted if it's current size is larger than the
    min size.
    """
    if self._pending_endpoints and not force:
      return

    num_healthy = len([c for c in self._heap[1:] if c.channel.is_open])
    if num_healthy > self._min_size:
      least_loaded_endpoint = None
      # Scan the heap for any closed endpoints.
      for n in self._heap[1:]:
        if n.channel.is_closed and n.endpoint not in self._pending_endpoints:
          least_loaded_endpoint = n.endpoint
          break
      if not least_loaded_endpoint:
        # Scan the heap for the least-loaded node.  This isn't exactly in-order,
        # but "close enough"
        for n in self._heap[1:]:
          if n.endpoint not in self._pending_endpoints:
            least_loaded_endpoint = n.endpoint
            break

      if least_loaded_endpoint:
        self._idle_endpoints.add(least_loaded_endpoint)
        super(ApertureBalancerSink, self)._RemoveSink(least_loaded_endpoint)
        self._log.debug('Contracting aperture to remove %s' % str(least_loaded_endpoint))
        self._UpdateSizeVarz()

  def _OnNodeDown(self, node):
    """Invoked by the base class when a node is marked down.
    In this case, if the downed node is currently in the aperture, we want to
    remove if, and then attempt to adjust the aperture.
    """
    if node.channel.state != ChannelState.Idle:
      ar, _ = self._TryExpandAperture()
      return ar
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

  def _ScheduleNextJitter(self):
    """Schedule the aperture to jitter in a random amount of time between
    _jitter_min and _jitter_max.
    """
    next_jitter = random.randint(self._jitter_min, self._jitter_max)
    now = LOW_RESOLUTION_TIME_SOURCE.now
    self._next_jitter = LOW_RESOLUTION_TIMER_QUEUE.Schedule(
        now + next_jitter, self._Jitter)

  def _Jitter(self):
    """Attempt to expand the aperture by one node, and if successful,
    contract it by a node (excluding the one that was just added).  This is
    done asynchronously.
    """
    try:
      ar, endpoint = self._TryExpandAperture(True)
      if endpoint:
        try:
          ar.wait()
          if not ar.exception:
            self._ContractAperture(True)
        finally:
          self._pending_endpoints.discard(endpoint)
    finally:
      self._ScheduleNextJitter()

  def _AdjustAperture(self, amount):
    """Adjusts the load average of the pool, and adjusts the aperture size
    if required by the new load average.

    Args:
      amount - The amount to change the load by.  May be +/-1
    """
    self._total += amount
    avg = self._ema.Update(self._time.Sample(), self._total)
    aperture_size = self._size
    if aperture_size == 0:
      # Essentially infinite load.
      aperture_load = self._max_load
    else:
      aperture_load = avg / aperture_size
      self.__varz.load_average(aperture_load)
    if (aperture_load >= self._max_load
        and self._idle_endpoints
        and aperture_size < self._max_size):
      self._TryExpandAperture()
    elif aperture_load <= self._min_load and aperture_size > self._min_size:
      self._ContractAperture()

ApertureBalancerSink.Builder = SinkProvider(
  ApertureBalancerSink,
  SinkRole.LoadBalancer,
  smoothing_window = 5,
  min_size = 1,
  max_size = 2**31,
  min_load = 0.5,
  max_load = 2.0,
  server_set_provider = None,
  jitter_min_sec = 120,
  jitter_max_sec = 240)
