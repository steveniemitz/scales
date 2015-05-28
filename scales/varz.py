from __future__ import absolute_import

from contextlib import contextmanager
from collections import (
  defaultdict,
  deque
)
import random
import time

class VarzType(object):
  Gauge = 1
  Rate = 2
  AggregateTimer = 3
  Counter = 4
  AverageTimer = 5

class SourceType(object):
  Method = 1
  Service = 2
  Endpoint = 4

  ServiceAndEndpoint = Service | Endpoint
  MethodAndService = Method | Service

class VarzMetric(object):
  VARZ_TYPE = None

  def __init__(self, metric, source):
    self._metric = metric
    self._source = source
    if self.VARZ_TYPE == VarzType.Gauge:
      self._fn = VarzReceiver.SetVarz
    elif self.VARZ_TYPE == VarzType.AverageTimer:
      self._fn = VarzReceiver.RecordTimerSample
    else:
      self._fn = VarzReceiver.IncrementVarz

  def __call__(self, *args):
    if self._source and len(args) == 1:
      source = self._source
      amount = args[0]
    elif self._source and len(args) == 0:
      source = self._source
      amount = 1
    elif not self._source and len(args) == 1:
      source = args[0]
      amount = 1
    elif len(args) == 2:
      source, amount = args
    else:
      raise Exception("Invalid arguments")
    self._fn(source, self._metric, amount)

  def ForSource(self, source):
    return type(self)(self._metric, source)

class Gauge(VarzMetric): VARZ_TYPE = VarzType.Gauge
class Rate(VarzMetric): VARZ_TYPE = VarzType.Rate
class Counter(Rate): VARZ_TYPE = VarzType.Counter

class VarzTimerBase(VarzMetric):
  @contextmanager
  def Measure(self):
    start_time = time.time()
    yield
    end_time = time.time()
    self(end_time - start_time)

class AverageTimer(VarzTimerBase): VARZ_TYPE = VarzType.AverageTimer
class AggregateTimer(VarzTimerBase): VARZ_TYPE = VarzType.AggregateTimer

class VarzMeta(type):
  def __init__(cls, name, bases, dct):
    base_name = cls._VARZ_BASE_NAME
    source_type = cls._VARZ_SOURCE_TYPE
    for metric_suffix, varz_cls in cls._VARZ.iteritems():
      metric_name = '%s.%s' % (base_name, metric_suffix)
      VarzReceiver.RegisterMetric(metric_name, varz_cls.VARZ_TYPE, source_type)
      varz = varz_cls(metric_name, None)
      cls._VARZ[metric_suffix] = varz
      setattr(cls, metric_suffix, varz)
    super(VarzMeta, cls).__init__(name, bases, dct)


class VarzBase(object):
  __metaclass__ = VarzMeta
  _VARZ = {}
  _VARZ_BASE_NAME = None
  _VARZ_SOURCE_TYPE = SourceType.Service

  def __init__(self, source):
    if not isinstance(source, tuple):
      source = source,
    for k, v in self._VARZ.iteritems():
      setattr(self, k, v.ForSource(source))


class VarzReceiver(object):
  """A stub class to receive varz from Scales."""
  METRICS = {}

  VARZ_DATA = defaultdict(lambda: defaultdict(int))

  _PERCENTILE_P = 1
  _MAX_PERCENTILE_BUCKET = 100000

  @staticmethod
  def RegisterMetric(metric, varz_type, source_type):
    VarzReceiver.METRICS[metric] = (varz_type, source_type)

  @staticmethod
  def IncrementVarz(source, metric, amount=1):
    """Increment (source, metric) by amount"""
    VarzReceiver.VARZ_DATA[metric][source] += amount

  @staticmethod
  def SetVarz(source, metric, value):
    """Set (source, metric) to value"""
    VarzReceiver.VARZ_DATA[metric][source] = value

  @staticmethod
  def RecordTimerSample(source, metric, value):
    if random.random() > VarzReceiver._PERCENTILE_P:
      return

    queue = VarzReceiver.VARZ_DATA[metric][source]
    if queue == 0:
      queue = deque()
      VarzReceiver.VARZ_DATA[metric][source] = queue

    if len(queue) > VarzReceiver._MAX_PERCENTILE_BUCKET:
      queue.popleft()
    queue.append(value)

class VarzSocketWrapper(object):
  """A wrapper for Thrift sockets that records various varz about the socket."""
  class Varz(VarzBase):
    _VARZ_BASE_NAME = 'scales.socket'
    _VARZ_SOURCE_TYPE = SourceType.ServiceAndEndpoint
    _VARZ = {
      'bytes_recv': Rate,
      'bytes_sent': Rate,
      'num_connections': Gauge,
      'tests_failed': Counter,
      'connects': Counter,
      'open_latency': AverageTimer
    }

  def __init__(self, socket, varz_tag, test_connections=False):
    self._socket = socket
    self._test_connections = test_connections
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

  def flush(self):
    pass

  def write(self, buff):
    self._socket.write(buff)
    self._varz.bytes_sent(len(buff))

  def open(self):
    with self._varz.open_latency.Measure():
      self._socket.open()
    self._varz.connects()
    self._varz.num_connections(1)

  def close(self):
    self._varz.num_connections(-1)
    self._socket.close()

  def readAll(self, sz):
    buff = ''
    have = 0
    while have < sz:
      chunk = self.read(sz - have)
      have += len(chunk)
      buff += chunk
      if len(chunk) == 0:
        raise EOFError()
    return buff

  def testConnection(self):
    if not self._test_connections:
      return True

    from gevent.select import select as gselect
    import select
    try:
      reads, _, _ = gselect([self._socket.handle], [], [], 0)
      return True
    except select.error:
      self._varz.tests_failed()
      return False
