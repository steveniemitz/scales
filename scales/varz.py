from __future__ import absolute_import

from contextlib import contextmanager
from collections import defaultdict, deque, namedtuple
import functools
import itertools
import math
import random
import socket
import time

import gevent

from .timer_queue import LOW_RESOLUTION_TIME_SOURCE

class VarzType(object):
  Gauge = 1
  Rate = 2
  AggregateTimer = 3
  Counter = 4
  AverageTimer = 5
  AverageRate = 6

class Source(object):
  __slots__ = "method", "service", "endpoint", "client_id"
  def __init__(self, method=None, service=None, endpoint=None, client_id=None):
    self.method = method
    self.service = service
    self.endpoint = endpoint
    self.client_id = client_id

  def to_tuple(self):
    return (self.method, self.service, self.endpoint, self.client_id)

  def to_dict(self):
    return {
      "method": self.method,
      "service": self.service,
      "endpoint": self.endpoint,
      "client_id": self.client_id,
    }

  def __cmp__(self, other):
    if not isinstance(other, Source):
      return -1
    return cmp(self.to_tuple(), other.to_tuple())

  def __hash__(self):
    return hash((self.method, self.service, self.endpoint, self.client_id))

class VarzMetric(object):
  """An object that can be used to record specific varz."""
  VARZ_TYPE = None

  @staticmethod
  def _Adapt(fn):
    def __Adapt(metric, source, amount=1):
      fn(source, metric, amount)
    return __Adapt

  def __init__(self, metric, source):
    """
    Args:
      metric - The metric name.
      source - An optional source.
    """
    self._metric = metric
    self._source = source
    if self.VARZ_TYPE == VarzType.Gauge:
      self._fn = VarzReceiver.SetVarz
    elif self.VARZ_TYPE in (VarzType.AverageTimer, VarzType.AverageRate):
      self._fn = VarzReceiver.RecordPercentileSample
    else:
      self._fn = VarzReceiver.IncrementVarz

    if source:
      self._fn = functools.partial(self._fn, self._source)
    else:
      self._fn = self._Adapt(self._fn)

  def __call__(self, *args):
    self._fn(self._metric, *args)

  def ForSource(self, source):
    """Specialize this metric for a specific source.

    Args:
      source - The source to specialize for.
    Returns:
      A VarzMetric of the same type.
    """
    if not isinstance(source, Source):
      raise ValueError("Source must be a source object", type(source))
    return type(self)(self._metric, source)

class Gauge(VarzMetric): VARZ_TYPE = VarzType.Gauge
class Rate(VarzMetric): VARZ_TYPE = VarzType.Rate
class AverageRate(VarzMetric): VARZ_TYPE = VarzType.AverageRate
class Counter(Rate): VARZ_TYPE = VarzType.Counter

class VarzTimerBase(VarzMetric):
  """A specialization of VarzMetric that also includes a contextmanager
  to time blocks of code."""
  @contextmanager
  def Measure(self, source=None):
    start_time = time.time()
    yield
    end_time = time.time()
    if source:
      self(source, end_time - start_time)
    else:
      self(end_time - start_time)

class AverageTimer(VarzTimerBase): VARZ_TYPE = VarzType.AverageTimer
class AggregateTimer(VarzTimerBase): VARZ_TYPE = VarzType.AggregateTimer

class VarzMeta(type):
  def __new__(mcs, name, bases, dct):
    base_name = dct['_VARZ_BASE_NAME']
    for metric_suffix, varz_cls in dct['_VARZ'].iteritems():
      metric_name = '%s.%s' % (base_name, metric_suffix)
      VarzReceiver.RegisterMetric(metric_name, varz_cls.VARZ_TYPE)
      varz = varz_cls(metric_name, None)
      dct['_VARZ'][metric_suffix] = varz
      dct[metric_suffix] = varz
    return super(VarzMeta, mcs).__new__(mcs, name, bases, dct)

def VerifySource(source):
  if not isinstance(source, Source):
    raise ValueError("InvalidSource", source)
  return source

class _VarzBase(object):
  """A helper class to create a set of Varz.

  Inheritors should set _VARZ_BASE_NAME.  Once done, the Varz object can be
  instantiated with a source, and then metrics invoked as attributes on the
  object.

  For example:
    class Varz(VarzBase):
      _VARZ_BASE_NAME = 'scales.example.varz'
      _VARZ = {
        'a_counter': Counter,
        'a_gauge': Gauge
      }

    my_varz = Varz(Source(service='my_service'))
    my_varz.a_counter()
    my_varz.a_gauge(5)
  """

  _VARZ = {}
  _VARZ_BASE_NAME = None

  def __init__(self, source):
    source = VerifySource(source)
    for k, v in self._VARZ.iteritems():
      setattr(self, k, v.ForSource(source))

  def __getattr__(self, item):
    return self._VARZ[item]

VarzBase = VarzMeta(
  '_VarzBase',
  (_VarzBase,),
  {
    '_VARZ_BASE_NAME': _VarzBase._VARZ_BASE_NAME,
    '_VARZ': _VarzBase._VARZ
  }
)

class _SampleSet(object):
  __slots__ = ('data', 'i', 'p', 'max_size', 'last_update')

  def __init__(self, max_size, data=None, p=.1):
    data = data or []
    self.data = deque(data, max_size)
    self.i = len(data)
    self.p = p
    self.max_size = max_size
    self.last_update = LOW_RESOLUTION_TIME_SOURCE.now

  def Sample(self, value):
    if self.i < self.max_size:
      self.data.append(value)
      self.last_update = LOW_RESOLUTION_TIME_SOURCE.now
    else:
      j = random.random()
      if j < self.p:
        self.data.append(value)
        self.last_update = LOW_RESOLUTION_TIME_SOURCE.now
    self.i += 1

class VarzReceiver(object):
  """A stub class to receive varz from Scales."""
  VARZ_METRICS = {}
  VARZ_DATA = defaultdict(lambda: defaultdict(int))
  VARZ_PERCENTILES = [.5, .90, .99, .999, .9999]

  _MAX_PERCENTILE_SIZE = 1000

  @staticmethod
  def RegisterMetric(metric, varz_type):
    VarzReceiver.VARZ_METRICS[metric] = varz_type

  @staticmethod
  def IncrementVarz(source, metric, amount=1):
    """Increment (source, metric) by amount"""
    VarzReceiver.VARZ_DATA[metric][VerifySource(source)] += amount

  @staticmethod
  def SetVarz(source, metric, value):
    """Set (source, metric) to value"""
    VarzReceiver.VARZ_DATA[metric][VerifySource(source)] = value

  @classmethod
  def RecordPercentileSample(cls, source, metric, value):
    source = VerifySource(source)
    reservoir = cls.VARZ_DATA[metric][source]
    if reservoir == 0:
      reservoir = _SampleSet(cls._MAX_PERCENTILE_SIZE)
      cls.VARZ_DATA[metric][source] = reservoir
    reservoir.Sample(value)

def DefaultKeySelector(k):
  """A key selector to use for Aggregate."""
  VerifySource(k)
  return k.service, k.client_id

class VarzAggregator(object):
  """An aggregator that rolls metrics up to the service level."""
  MAX_AGG_AGE = 5 * 60

  class _Agg(object):
    __slots__ = 'total', 'count', 'work'
    def __init__(self):
      self.total = 0.0
      self.count = 0
      self.work = 0.0

  @staticmethod
  def CalculatePercentile(values, pct):
    if not values:
      return 0

    k = (len(values) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
      return values[int(k)]
    d0 = values[int(f)] * (c - k)
    d1 = values[int(c)] * (k - f)
    return d0 + d1

  @staticmethod
  def _Downsample(lst, target_size):
    if target_size == 0:
      return
    elif len(lst) < 3 or len(lst) <= target_size:
      for n in lst:
        yield n
    else:
      skip = len(lst) / target_size
      lst = sorted(lst)
      for i, n in enumerate(lst[0:-2]):
        if i % skip == 0:
          yield n
      yield lst[-1]

  @staticmethod
  def Aggregate(varz, metrics, key_selector=None):
    """Aggregate varz

    Args:
      varz - The varz dictionary to aggregate, typically VarzReceiver.VARZ_DATA.
      metrics - The metric metadata dictionary, typically VarzReceiver.VARZ_METRICS
      key_selector - A function to select the key to aggregate by given a
                     (endpoint, service, host, client) tuple.  By default this returns
                     the service.
    Returns:
      A dictionary of metric -> service -> aggregate.
    """
    if not key_selector:
      key_selector = DefaultKeySelector

    agg = defaultdict(dict)
    now = LOW_RESOLUTION_TIME_SOURCE.now
    for metric in varz.keys():
      if metric not in metrics:
        continue
      varz_type = metrics[metric]
      assert isinstance(varz_type, int), varz_type
      metric_agg = agg[metric]
      gevent.sleep(0)
      for source in varz[metric].keys():
        key = key_selector(source)
        data = varz[metric][source]
        if key not in metric_agg:
          metric_agg[key] = VarzAggregator._Agg()
          if isinstance(data, _SampleSet):
            metric_agg[key].work = []

        if isinstance(data, _SampleSet):
          if (now - data.last_update) < VarzAggregator.MAX_AGG_AGE:
            metric_agg[key].work.append(data)
            metric_agg[key].count += 1
        else:
          metric_agg[key].work += data
          metric_agg[key].count += 1
      if varz_type in (VarzType.AggregateTimer, VarzType.Counter,
                       VarzType.Gauge, VarzType.Rate):
        for source_agg in metric_agg.values():
          source_agg.total = source_agg.work
      elif varz_type in (VarzType.AverageTimer, VarzType.AverageRate):
        for source_agg in metric_agg.values():
          if source_agg.count > 0:
            pct_sample = 1.0 / source_agg.count
            values = [VarzAggregator._Downsample(v.data, int(len(v.data) * pct_sample))
                      for v in source_agg.work]
            values = sorted(itertools.chain(*values))
          else:
            values = []
          source_agg.total = [
            VarzAggregator.CalculatePercentile(values, pct)
            for pct in VarzReceiver.VARZ_PERCENTILES
            ]
          if values:
            source_agg.total.insert(0, sum(values) / float(len(values)))
          else:
            source_agg.total.insert(0, 0)
          source_agg.work = None
      else:
        for source_agg in metric_agg.values():
          source_agg.total = float(source_agg.work) / source_agg.count

    return agg

class VarzSocketWrapper(object):
  """A wrapper for Thrift sockets that records various varz about the socket."""
  __slots__ = ('_socket', '_varz', '_is_open')

  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.socket'
    _VARZ = {
      'bytes_recv': Rate,
      'bytes_sent': Rate,
      'num_connections': Counter,
      'tests_failed': Counter,
      'connects': Rate,
      'open_latency': AverageTimer
    }

  def __init__(self, socket, varz_tag):
    self._socket = socket
    self._is_open = self._socket.isOpen()
    self._varz = self.Varz(Source(service=varz_tag, endpoint='%s:%d' % (self.host, self.port)))

  @property
  def host(self):
    return self._socket.host

  @property
  def port(self):
    return self._socket.port

  def isOpen(self):
    return self._socket.isOpen()

  def read(self, sz):
    buff = self._socket.read(sz)
    self._varz.bytes_recv(len(buff))
    return buff

  def recv_into(self, buf, sz):
    return self._socket.handle.recv_into(buf, sz)

  def flush(self):
    pass

  def write(self, buff):
    self._socket.handle.sendall(buff)
    self._varz.bytes_sent(len(buff))

  def open(self):
    with self._varz.open_latency.Measure():
      self._socket.open()

    if self._socket.handle:
      # Disabling nagling (enabling TCP_NODELAY) causes the kernel to not buffer
      # small writes for up to 40 (implementation dependent)ms.  This improves
      # request latency for small requests.
      self._socket.handle.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    self._is_open = True
    self._varz.connects()
    self._varz.num_connections(1)

  def close(self):
    if self._is_open:
      self._is_open = False
      self._varz.num_connections(-1)
      self._socket.close()

  def readAll(self, sz):
    buff = bytearray(sz)
    view = memoryview(buff)
    have = 0
    while have < sz:
      read_size = sz - have
      chunk_len = self.recv_into(view[have:], read_size)
      have += chunk_len
      if chunk_len == 0:
        raise EOFError()
    self._varz.bytes_recv(sz)
    return buff


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
    self.value = 0.0

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
      self.value = float(sample)
    else:
      delta = ts - self._time
      self._time = ts
      window = 0 if self._window == 0 else math.exp(-float(delta) / self._window)
      self.value = (sample * (1-window)) + (self.value * window)
    return self.value
