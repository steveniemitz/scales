import itertools
import random
import unittest

from scales.varz import (
  _Reservoir,
  SourceType,
  VarzAggregator,
  VarzReceiver,
  VarzType,
)

def _round(arr, n):
  return [round(i, n) for i in arr]

class VarzTestCase(unittest.TestCase):
  def _getSampleData(self):
    test_varz = {
      'metric1': {
        (None, 'service1', 'enpoint1'): 1,
        (None, 'service1', 'enpoint2'): 2
      },
      'metric2': {
        (None, 'service1', 'endpoint1'): 1,
        (None, 'service2', 'endpoint1'): 2
      },
      'metric3': {
        ('method1', 'service1', None): 1,
        ('method2', 'service1', None): 2,
      }
    }
    return test_varz

  def _getSampleRateData(self):
    test_varz = {
      'metric1': {
        (None, 'service1', 'enpoint1'): _Reservoir([1,2]),
        (None, 'service1', 'enpoint2'): _Reservoir([2,3])
      },
      'metric2': {
        (None, 'service1', 'endpoint1'): _Reservoir([1,2]),
        (None, 'service2', 'endpoint1'): _Reservoir([2,3])
      },
      'metric3': {
        ('method1', 'service1', None): _Reservoir([1,2]),
        ('method2', 'service1', None): _Reservoir([2,3]),
      }
    }
    return test_varz

  def testVarzAggregatorBasic(self):
    test_varz = self._getSampleData()
    metrics = {
      'metric1': (VarzType.Counter, SourceType.ServiceAndEndpoint),
      'metric2': (VarzType.Counter, SourceType.ServiceAndEndpoint),
      'metric3': (VarzType.Counter, SourceType.MethodAndService)
    }
    aggs = VarzAggregator.Aggregate(test_varz, metrics)
    # Endpoints are aggregated to a service
    self.assertEqual(aggs['metric1']['service1'].total, 3.0)
    # Services are kept separate
    self.assertEqual(aggs['metric2']['service1'].total, 1.0)
    self.assertEqual(aggs['metric2']['service2'].total, 2.0)
    # Method are aggregated to a service
    self.assertEqual(aggs['metric3']['service1'].total, 3.0)

  def testVarzAggregatorAverageRate(self):
    test_varz = self._getSampleRateData()
    metrics = {
      'metric1': (VarzType.AverageRate, SourceType.ServiceAndEndpoint),
      'metric2': (VarzType.AverageRate, SourceType.ServiceAndEndpoint),
      'metric3': (VarzType.AverageRate, SourceType.MethodAndService)
    }
    random.seed(1)
    aggs = VarzAggregator.Aggregate(test_varz, metrics)
    self.assertEqual(_round(aggs['metric1']['service1'].total, 2), [2.0, 2.0, 2.70, 2.97, 3.0, 3.0])
    self.assertEqual(_round(aggs['metric2']['service1'].total, 2), [1.5, 1.5, 1.90, 1.99, 2.0, 2.0])
    self.assertEqual(_round(aggs['metric2']['service2'].total, 2), [2.5, 2.5, 2.90, 2.99, 3.0, 3.0])
    self.assertEqual(_round(aggs['metric3']['service1'].total, 2), [2.0, 2.0, 2.70, 2.97, 3.0, 3.0])

  def testShortStreamingPercentile(self):
    source = (None, 'test', None)
    metric = 'test'
    VarzReceiver.VARZ_METRICS.clear()
    VarzReceiver.VARZ_DATA.clear()
    VarzReceiver.RegisterMetric(metric, VarzType.AverageTimer, SourceType.Service)
    VarzReceiver.RecordPercentileSample(source, metric, 1)
    VarzReceiver.RecordPercentileSample(source, metric, 2)
    VarzReceiver.RecordPercentileSample(source, metric, 3)
    VarzReceiver.RecordPercentileSample(source, metric, 2)
    aggs = VarzAggregator.Aggregate(VarzReceiver.VARZ_DATA, VarzReceiver.VARZ_METRICS)
    self.assertEqual(
      _round(aggs[metric]['test'].total, 2),
      [2.0, 2.0, 2.70, 2.97, 3.0, 3.0])

  def testLongStreamingPercentile(self):
    source = (None, 'test', None)
    metric = 'test'
    VarzReceiver.VARZ_METRICS.clear()
    VarzReceiver.VARZ_DATA.clear()

    VarzReceiver.RegisterMetric(metric, VarzType.AverageTimer, SourceType.Service)
    random.seed(1)
    for n in xrange(10000):
      VarzReceiver.RecordPercentileSample(source, metric, random.randint(0, 100))

    aggs = VarzAggregator.Aggregate(VarzReceiver.VARZ_DATA, VarzReceiver.VARZ_METRICS)
    self.assertEqual(
      _round(aggs[metric]['test'].total, 2),
      [49.07, 48.0, 90.0, 99.0, 100.0, 100.0])

if __name__ == '__main__':
  unittest.main()
