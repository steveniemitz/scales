from __future__ import absolute_import

from contextlib import contextmanager
from collections import (
  defaultdict,
  deque
)
import functools
import math
import random
import time

import gevent

class VarzType(object):
  Gauge = 1
  Rate = 2
  AggregateTimer = 3
  Counter = 4
  AverageTimer = 5
  AverageRate = 6

class SourceType(object):
  Method = 1
  Service = 2
  Endpoint = 4

  ServiceAndEndpoint = Service | Endpoint
  MethodAndService = Method | Service

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
    if not isinstance(source, tuple) or len(source) != 3:
      raise Exception("Source must be a 3-tuple")

    return type(self)(self._metric, source)

class Gauge(VarzMetric): VARZ_TYPE = VarzType.Gauge
class Rate(VarzMetric): VARZ_TYPE = VarzType.Rate
class AverageRate(VarzMetric): VARZ_TYPE = VarzType.AverageRate
class Counter(Rate): VARZ_TYPE = VarzType.Counter

class VarzTimerBase(VarzMetric):
  """A specialization of VarzMetric that also includes a contextmanager
  to time blocks of code."""
  @contextmanager
  def Measure(self):
    start_time = time.time()
    yield
    end_time = time.time()
    self(end_time - start_time)

class AverageTimer(VarzTimerBase): VARZ_TYPE = VarzType.AverageTimer
class AggregateTimer(VarzTimerBase): VARZ_TYPE = VarzType.AggregateTimer

class VarzMeta(type):
  def __new__(mcs, name, bases, dct):
    base_name = dct['_VARZ_BASE_NAME']
    source_type = dct.get('_VARZ_SOURCE_TYPE', SourceType.Service)
    for metric_suffix, varz_cls in dct['_VARZ'].iteritems():
      metric_name = '%s.%s' % (base_name, metric_suffix)
      VarzReceiver.RegisterMetric(metric_name, varz_cls.VARZ_TYPE, source_type)
      varz = varz_cls(metric_name, None)
      dct['_VARZ'][metric_suffix] = varz
      dct[metric_suffix] = varz
    return super(VarzMeta, mcs).__new__(mcs, name, bases, dct)


class VarzBase(object):
  """A helper class to create a set of Varz.

  Inheritors should set _VARZ_BASE_NAME as well as _VARZ_SOURCE_TYPE.  Once done,
  the Varz object can be instantiated with a source, and then metrics invoked as
  attributes on the object.

  For example:
    class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.example.varz'
    _VARZ = {
      'a_counter': Counter,
      'a_gauge': Gauge
    }

    my_varz = Varz('source1')
    my_varz.a_counter()
    my_varz.a_gauge(5)
  """

  __metaclass__ = VarzMeta
  _VARZ = {}
  _VARZ_BASE_NAME = None
  _VARZ_SOURCE_TYPE = SourceType.Service

  def __init__(self, source):
    method, service, endpoint = None, None, None
    if self._VARZ_SOURCE_TYPE == SourceType.Service:
      service = source
    elif self._VARZ_SOURCE_TYPE == SourceType.MethodAndService:
      method, service = source
    elif self._VARZ_SOURCE_TYPE == SourceType.ServiceAndEndpoint:
      service, endpoint = source
    source = method, service, endpoint
    for k, v in self._VARZ.iteritems():
      setattr(self, k, v.ForSource(source))

  def __getattr__(self, item):
    return self._VARZ[item]


class VarzReceiver(object):
  """A stub class to receive varz from Scales."""
  VARZ_METRICS = {}
  VARZ_DATA = defaultdict(lambda: defaultdict(int))
  VARZ_DATA_PERCENTILES = defaultdict(lambda: defaultdict(float))
  VARZ_PERCENTILES = [.5, .90, .95, .99]

  _PERCENTILE_P = .1
  _MAX_PERCENTILE_BUCKET = 1000

  @staticmethod
  def RegisterMetric(metric, varz_type, source_type):
    VarzReceiver.VARZ_METRICS[metric] = (varz_type, source_type)

  @staticmethod
  def IncrementVarz(source, metric, amount=1):
    """Increment (source, metric) by amount"""
    VarzReceiver.VARZ_DATA[metric][source] += amount

  @staticmethod
  def SetVarz(source, metric, value):
    """Set (source, metric) to value"""
    VarzReceiver.VARZ_DATA[metric][source] = value

  @classmethod
  def RecordPercentileSample(cls, source, metric, value):
    if random.random() > VarzReceiver._PERCENTILE_P:
      return

    queue = cls.VARZ_DATA[metric][source]
    if queue == 0:
      queue = deque()
      cls.VARZ_DATA[metric][source] = queue

    if len(queue) > cls._MAX_PERCENTILE_BUCKET:
      queue.popleft()
    queue.append(value)

class VarzAggregator(object):
  """An aggregator that rolls metrics up to the service level."""
  class _Agg(object):
    __slots__ = 'total', 'count', 'work'
    def __init__(self):
      self.total = 0.0
      self.count = 0
      self.work = 0.0

  @staticmethod
  def CalculatePercentile(values, pct):
    k = (len(values) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
      return values[int(k)]
    d0 = values[int(f)] * (c - k)
    d1 = values[int(c)] * (k - f)
    return d0 + d1

  @staticmethod
  def Aggregate(varz, metrics):
    """Aggregate varz

    Args:
      varz - The varz dictionary to aggregate, typically VarzReceiver.VARZ_DATA.
      metrics - The metric metadata dictionary, typically VarzReceiver.VARZ_METRICS
    Returns:
      A dictionary of metric -> service -> aggregate.
    """

    agg = defaultdict(dict)
    for metric in varz.keys():
      metric_info = metrics.get(metric, None)
      if not metric_info:
        continue
      varz_type, source_tpe = metric_info
      metric_agg = agg[metric]
      for source in varz[metric].keys():
        gevent.sleep(0)
        method, service, endpoint = source
        data = varz[metric][source]
        if service not in metric_agg:
          metric_agg[service] = VarzAggregator._Agg()
          if isinstance(data, deque):
            metric_agg[service].work = []

        if isinstance(data, deque):
          metric_agg[service].work.append(data)
        else:
          metric_agg[service].work += data
        metric_agg[service].count += 1

      if varz_type in (VarzType.AggregateTimer, VarzType.Counter,
                       VarzType.Gauge, VarzType.Rate):
        for source_agg in metric_agg.values():
          source_agg.total = source_agg.work
      elif varz_type in (VarzType.AverageTimer, VarzType.AverageRate):
        for source_agg in metric_agg.values():
          pct_sample = 1.0 / source_agg.count
          values = []
          for v in source_agg.work:
            values.extend(random.sample(v, int(len(v) * pct_sample)))
          values = sorted(values)

          source_agg.total = [
            VarzAggregator.CalculatePercentile(values, pct)
            for pct in VarzReceiver.VARZ_PERCENTILES
          ]
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
    _VARZ_SOURCE_TYPE = SourceType.ServiceAndEndpoint
    _VARZ = {
      'bytes_recv': Rate,
      'bytes_sent': Rate,
      'num_connections': Counter,
      'tests_failed': Counter,
      'connects': Counter,
      'open_latency': AverageTimer
    }

  def __init__(self, socket, varz_tag):
    self._socket = socket
    self._is_open = self._socket.isOpen()
    self._varz = self.Varz((varz_tag, '%s:%d' % (self.host, self.port)))

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
