import itertools
import random
import unittest

from scales.varz import (
  _SampleSet,
  Source,
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
        Source(None, 'service1', 'enpoint1', None): 1,
        Source(None, 'service1', 'enpoint2', None): 2
      },
      'metric2': {
        Source(None, 'service1', 'endpoint1', None): 1,
        Source(None, 'service2', 'endpoint1', None): 2
      },
      'metric3': {
        Source('method1', 'service1', None, None): 1,
        Source('method2', 'service1', None, None): 2,
      },
      'metric4': {
        Source(None, "service1", None, "client1"): 1,
        Source(None, "service1", None, "client2"): 2,
        Source(None, "service2", None, "client1"): 3,
      },
    }
    return test_varz

  def _getSampleRateData(self):
    test_varz = {
      'metric1': {
        Source(None, 'service1', 'enpoint1', None): _SampleSet(2, [1,2]),
        Source(None, 'service1', 'enpoint2', None): _SampleSet(2, [2,3])
      },
      'metric2': {
        Source(None, 'service1', 'endpoint1', None): _SampleSet(2, [1,2]),
        Source(None, 'service2', 'endpoint1', None): _SampleSet(2, [2,3])
      },
      'metric3': {
        Source('method1', 'service1', None, None): _SampleSet(2, [1,2]),
        Source('method2', 'service1', None, None): _SampleSet(2, [2,3]),
      },
      'metric4': {
        Source(None, 'service1', None, 'client1'): _SampleSet(2, [1,2]),
        Source(None, 'service1', None, 'client2'): _SampleSet(2, [2,3]),
        Source(None, 'service2', None, 'client1'): _SampleSet(2, [4,5])
      },
    }
    return test_varz

  def testVarzAggregatorBasic(self):
    test_varz = self._getSampleData()
    metrics = {
      'metric1': VarzType.Counter,
      'metric2': VarzType.Counter,
      'metric3': VarzType.Counter,
      'metric4': VarzType.Counter,
    }
    aggs = VarzAggregator.Aggregate(test_varz, metrics)
    # Endpoints are aggregated to a service
    self.assertEqual(aggs['metric1'][('service1', None)].total, 3.0)
    # Services are kept separate
    self.assertEqual(aggs['metric2'][('service1', None)].total, 1.0)
    self.assertEqual(aggs['metric2'][('service2', None)].total, 2.0)
    # Method are aggregated to a service
    self.assertEqual(aggs['metric3'][('service1', None)].total, 3.0)
    # Methods are agregated to a client + service
    self.assertEqual(aggs['metric4'][('service1', 'client1')].total, 1.0)
    self.assertEqual(aggs['metric4'][('service1', 'client2')].total, 2.0)
    self.assertEqual(aggs['metric4'][('service2', 'client1')].total, 3.0)

  def testVarzAggregatorAverageRate(self):
    test_varz = self._getSampleRateData()
    metrics = {
      'metric1': VarzType.AverageRate,
      'metric2': VarzType.AverageRate,
      'metric3': VarzType.AverageRate,
      'metric4': VarzType.AverageRate,
    }
    random.seed(1)
    aggs = VarzAggregator.Aggregate(test_varz, metrics)
    self.assertEqual(_round(aggs['metric1'][('service1', None)].total, 2), [2.0, 2.0, 2.70, 2.97, 3.0, 3.0])
    self.assertEqual(_round(aggs['metric2'][('service1', None)].total, 2), [1.5, 1.5, 1.90, 1.99, 2.0, 2.0])
    self.assertEqual(_round(aggs['metric2'][('service2', None)].total, 2), [2.5, 2.5, 2.90, 2.99, 3.0, 3.0])
    self.assertEqual(_round(aggs['metric3'][('service1', None)].total, 2), [2.0, 2.0, 2.70, 2.97, 3.0, 3.0])
    self.assertEqual(_round(aggs['metric4'][('service1', 'client1')].total, 2), [1.5, 1.5, 1.9, 1.99, 2.0, 2.0])
    self.assertEqual(_round(aggs['metric4'][('service1', 'client2')].total, 2), [2.5, 2.5, 2.9, 2.99, 3.0, 3.0])
    self.assertEqual(_round(aggs['metric4'][('service2', 'client1')].total, 2), [4.5, 4.5, 4.9, 4.99, 5.0, 5.0])

  def testShortStreamingPercentile(self):
    source = Source(None, 'test', None, None)
    metric = 'test'
    VarzReceiver.VARZ_METRICS.clear()
    VarzReceiver.VARZ_DATA.clear()
    VarzReceiver.RegisterMetric(metric, VarzType.AverageTimer)
    VarzReceiver.RecordPercentileSample(source, metric, 1)
    VarzReceiver.RecordPercentileSample(source, metric, 2)
    VarzReceiver.RecordPercentileSample(source, metric, 3)
    VarzReceiver.RecordPercentileSample(source, metric, 2)
    aggs = VarzAggregator.Aggregate(VarzReceiver.VARZ_DATA, VarzReceiver.VARZ_METRICS)
    self.assertEqual(
      _round(aggs[metric][('test', None)].total, 2),
      [2.0, 2.0, 2.70, 2.97, 3.0, 3.0])

  def testLongStreamingPercentile(self):
    source = Source(None, 'test', None, None)
    metric = 'test'
    VarzReceiver.VARZ_METRICS.clear()
    VarzReceiver.VARZ_DATA.clear()

    VarzReceiver.RegisterMetric(metric, VarzType.AverageTimer)
    random.seed(1)
    for n in xrange(10000):
      VarzReceiver.RecordPercentileSample(source, metric, float(random.randint(0, 100)))

    aggs = VarzAggregator.Aggregate(VarzReceiver.VARZ_DATA, VarzReceiver.VARZ_METRICS)
    self.assertEqual(
      _round(aggs[metric][('test', None)].total, 2),
      [50.25, 50, 92.0, 100.0, 100.0, 100.0])

if __name__ == '__main__':
  unittest.main()
