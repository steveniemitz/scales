import unittest

from scales.constants import SinkProperties
from scales.pool.singleton import SingletonPoolSink
from test.scales.util.base import SinkTestCase

class SingletonPoolTestCast(SinkTestCase):
  SINK_CLS = SingletonPoolSink

  def customize(self):
    self.sink_properties[SinkProperties.Endpoint] = 'localhost'

  def testSingletonPoolFowardsMessage(self):
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)

  def testSingletonPoolReusesSink(self):
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)

    self._prepareSinkStack()
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)
    self.assertEqual(len(self.mock_provider.sinks_created), 1)

  def testSingletonPoolRecreatesFailedSink(self):
    self.mock_provider.sinks_created[0].Fault()
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)
    self.assertEqual(len(self.mock_provider.sinks_created), 2)

if __name__ == '__main__':
  unittest.main()
