from collections import deque
import random
import unittest

from scales.varz import SourceType, VarzType, VarzAggregator

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
        (None, 'service1', 'enpoint1'): deque([1,2]),
        (None, 'service1', 'enpoint2'): deque([2,3])
      },
      'metric2': {
        (None, 'service1', 'endpoint1'): deque([1,2]),
        (None, 'service2', 'endpoint1'): deque([2,3])
      },
      'metric3': {
        ('method1', 'service1', None): deque([1,2]),
        ('method2', 'service1', None): deque([2,3]),
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
    self.assertEqual(_round(aggs['metric1']['service1'].total, 2), [2.5, 2.9, 2.95, 2.99])
    self.assertEqual(_round(aggs['metric2']['service1'].total, 2), [1.5, 1.9, 1.95, 1.99])
    self.assertEqual(_round(aggs['metric2']['service2'].total, 2), [2.5, 2.9, 2.95, 2.99])
    self.assertEqual(_round(aggs['metric3']['service1'].total, 2), [2.0, 2.0, 2.00, 2.00])

if __name__ == '__main__':
  unittest.main()
