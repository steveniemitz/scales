import unittest

import gevent

from scales.constants import SinkProperties, ChannelState
from scales.loadbalancer.zookeeper import Endpoint
from scales.pool.watermark import WatermarkPoolSink, ServiceClosedError
from test.scales.util.base import SinkTestCase

class WatermarkPoolTestCast(SinkTestCase):
  SINK_CLS = WatermarkPoolSink

  def customize(self, max_watermark=2):
    self.sink_properties = WatermarkPoolSink.Builder(max_watermark=max_watermark).sink_properties
    self.global_properties[SinkProperties.Endpoint] = Endpoint('localhost', 8080)

  def testWatermarkPoolForwardsMessage(self):
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)

  def testWatermarkPoolRecreatesFailedSink(self):
    self.mock_provider.sinks_created[0].Fault()
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)
    self.assertEqual(len(self.mock_provider.sinks_created), 2)

  def testWatermarkPoolReusesOpenSink(self):
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)

    self._prepareSinkStack()
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)
    self.assertEqual(len(self.mock_provider.sinks_created), 1)

  def testWatermarkPoolCreatesNewSinkAboveLowWatermark(self):
    self._submitTestMessage()
    self._submitTestMessage()
    self.assertEqual(len(self.mock_provider.sinks_created), 2)

  def testWatermarkPoolQueuesPastHighWatermark(self):
    self.setUp(max_watermark=1)
    self._submitTestMessage()
    self._submitTestMessage()
    self.assertEqual(len(self.mock_provider.sinks_created), 1)
    self.assertEqual(len(self.sink._waiters), 1)

  def _testQueueHelper(self, pre_process_response, asserts):
    self.setUp(max_watermark=1)

    msg_1 = object()
    msg_2 = object()
    r_msg_1 = object()
    r_msg_2 = object()

    ss_1 = self.sink_stack
    self._prepareSinkStack()
    ss_2 = self.sink_stack

    self.sink.AsyncProcessRequest(ss_1, msg_1, None, None)
    self.sink.AsyncProcessRequest(ss_2, msg_2, None, None)
    if pre_process_response:
      pre_process_response()

    ss_1.AsyncProcessResponseMessage(r_msg_1)
    asserts(ss_1, r_msg_1, ss_2, r_msg_2)

  def testWatermarkPoolProcessesQueue(self):
    def asserts(ss_1, r_msg_1, ss_2, r_msg_2):
      self.assertEqual(self.return_message, r_msg_1)
      self.assertEqual(ss_1.processed_response, True)
      self.assertEqual(ss_1.return_message, r_msg_1)
      self.assertEqual(ss_2.processed_response, False)
      # Yield to allow the queue to process
      gevent.sleep(0)
      ss_2.AsyncProcessResponseMessage(r_msg_2)
      self.assertEqual(ss_1.processed_response, True)
      self.assertEqual(ss_1.return_message, r_msg_1)
      self.assertEqual(ss_2.processed_response, True)
      self.assertEqual(ss_2.return_message, r_msg_2)

    self._testQueueHelper(None, asserts)

  def testWatermarkPoolFailureKillsQueue(self):
    def close_sink():
      self.mock_provider.sinks_created[0].state = ChannelState.Closed
    def asserts(ss_1, r_msg_1, ss_2, r_msg_2):
      self.assertEqual(self.return_message, r_msg_1)
      self.assertEqual(ss_1.processed_response, True)
      self.assertEqual(ss_1.return_message, r_msg_1)
      self.assertEqual(ss_2.processed_response, True)
      self.assertIsInstance(ss_2.return_message.error, ServiceClosedError)

    self._testQueueHelper(close_sink, asserts)

if __name__ == '__main__':
  unittest.main()
