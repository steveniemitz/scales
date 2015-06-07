import math
import random
import time

from .heap import HeapBalancerSink
from ..constants import (ChannelState, SinkProperties)
from ..sink import ChannelSinkProvider
from ..varz import (
  Gauge,
  SourceType,
  VarzBase
)

class MonoClock(object):
  def __init__(self):
    self._last = time.time()

  def Sample(self):
    now = time.time()
    if now - self._last > 0:
      self._last = now
    return self._last


class Ema(object):
  def __init__(self, window):
    self._window = window
    self._time = -1
    self._ema = 0

  def Update(self, ts, sample):
    if self._time == -1:
      self._time = ts
      self._ema = sample
    else:
      delta = ts - self._time
      self._time = ts
      window = 0 if self._window == 0 else math.exp(-float(delta) / self._window)
      self._ema = (sample * (1-window)) + (self._ema * window)
    return self._ema


class ApertureBalancerSink(HeapBalancerSink):
  class ApertureVarz(VarzBase):
    _VARZ_BASE_NAME = 'scales.pool.ApertureBalancer'
    _VARZ_SOURCE_TYPE = SourceType.Service
    _VARZ = {
      'idle': Gauge,
      'active': Gauge,
      'load_average': Gauge
    }

  def __init__(self, next_provider, properties):
    self._idle_channels = set()
    self._active_channels = set()
    self._total = 0
    self._ema = Ema(5)
    self._time = MonoClock()
    self._min_size = 1
    self._min_load = 0.5
    self._max_load = 2
    service_name = properties[SinkProperties.Service]
    self.__varz = self.ApertureVarz(service_name)
    super(ApertureBalancerSink, self).__init__(next_provider, properties)

  def _UpdateSizeVarz(self):
    self.__varz.active(len(self._active_channels))
    self.__varz.idle(len(self._idle_channels))

  def _AddNode(self, channel):
    num_healthy = len([c for c in self._active_channels if c.state <= ChannelState.Open])
    if num_healthy < self._min_size:
      self._active_channels.add(channel)
      super(ApertureBalancerSink, self)._AddNode(channel)
    else:
      self._idle_channels.add(channel)
    self._UpdateSizeVarz()

  def _RemoveNode(self, channel):
    super(ApertureBalancerSink, self)._RemoveNode(channel)
    if channel in self._active_channels:
      self._active_channels.discard(channel)
      self._TryExpandAperture()
    if channel in self._idle_channels:
      self._idle_channels.discard(channel)
    self._UpdateSizeVarz()

  def _TryExpandAperture(self):
    while any(self._idle_channels):
      new_channel, = random.sample(self._idle_channels, 1)
      self._idle_channels.discard(new_channel)
      if new_channel.state != ChannelState.Closed:
        self._log.debug('Expanding aperture.')
        self._active_channels.add(new_channel)
        super(ApertureBalancerSink, self)._AddNode(new_channel)
        break
    self._UpdateSizeVarz()

  def _ContractAperture(self):
    if len(self._active_channels) > 1:
      self._log.debug('Contracting aperture')
      # The end of the heap will be the most-loaded-ish.
      most_loaded = self._heap[-1].channel
      self._active_channels.discard(most_loaded)
      self._idle_channels.add(most_loaded)
      super(ApertureBalancerSink, self)._RemoveNode(most_loaded)
      self._UpdateSizeVarz()

  def _OnGet(self, node):
    self._AdjustAperture(1)

  def _OnPut(self, node):
    self._AdjustAperture(-1)

  def _AdjustAperture(self, amount):
    self._total += amount
    avg = self._ema.Update(self._time.Sample(), self._total)
    aperture_size = len(self._active_channels)
    aperture_load = avg / aperture_size
    self.__varz.load_average(aperture_load)
    if aperture_load >= self._max_load and any(self._idle_channels):
      self._TryExpandAperture()
    elif aperture_load <= self._min_load and aperture_size > self._min_size:
      self._ContractAperture()

ApertureBalancerChannelSinkProvider = ChannelSinkProvider(ApertureBalancerSink)
