import unittest

import gevent

from scales.constants import ChannelState
from scales.loadbalancer import HeapBalancerSink
from test.scales.loadbalancer.base_tests import LoadBalancerTestCase

class HeapBalancerTestCase(LoadBalancerTestCase):
  SINK_CLS = HeapBalancerSink

  def testHeapOpensAllChannels(self):
    self.assertTrue(all(s.state == ChannelState.Open
                        for s in self.mock_provider.sinks_created))

  def testHeapForwardsMessage(self):
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertIs(self.return_message, self.MSG_SENTINEL)

  def testHeapMaintainsLeastLoad(self):
    hs = self.sink
    self._submitTestMessage()
    self.assertEqual(hs._heap[1].load, hs.Zero)
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)

  def testHeapTracksLoad(self):
    self._submitTestMessage()
    self.assertTrue(any([n.load == self.sink.Zero + 1
                         for n in self.sink._heap[1:]]))
    self._completeTestMessage()
    self.assertTrue(all([n.load == self.sink.Zero
                         for n in self.sink._heap[1:]]))

  def testHeapRemovesNode(self):
    self._submitTestMessage()
    self._completeTestMessage()
    self.mock_ss_provider.RemoveServer('localhost', 8080)
    self.assertTrue(all(n.channel.endpoint.port != 8080 for n in self.sink._heap[1:]))

  def testHeapRemovesLoadedNode(self):
    self._submitTestMessage()
    loaded_node = self._getLoadedNode()
    loaded_ep = loaded_node.channel.endpoint
    self.mock_ss_provider.RemoveServer(loaded_ep.host, loaded_ep.port)

    # -1 means it was removed from the heap
    self.assertEqual(loaded_node.index, -1)
    # It should still have load
    self.assertEqual(loaded_node.load, self.sink.Zero + 1)
    # It shouldn't be in the heap anymore
    self.assertTrue(all(n.channel.endpoint.port != loaded_ep.port for n in self.sink._heap[1:]))
    return loaded_node

  def testHeapRemovedNodeReleaseCloses(self):
    loaded_node = self.testHeapRemovesLoadedNode()
    self._completeTestMessage()
    self.assertEqual(loaded_node.channel.state, ChannelState.Closed)

  def testHeapFailsOnEmpty(self):
    self.mock_ss_provider.RemoveAllServers()
    self._submitTestMessage()
    self.assertIsNotNone(self.return_message.error)

  def testHeapSucceedsWhenAllFailed(self):
    for s in self.mock_provider.sinks_created:
      s.state = ChannelState.Closed
    self._submitTestMessage()
    self.assertIsNotNone(self.return_message.error)

  def testHeapCorrectlyMarksNodesUp(self):
    self.testHeapSucceedsWhenAllFailed()
    self.mock_provider.sinks_created[0].state = ChannelState.Open
    self._prepareSinkStack()
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)

  def testHeapConcurrentOpenAndRequest(self):
    self.mock_ss_provider.RemoveAllServers()
    #self.mock_ss_provider.RemoveServer('localhost', 8080)
    self.sink_properties['open_delay'] = .1

    def race():
      gevent.sleep(.01)
      self._submitTestMessage()
      self._completeTestMessage()

    racer = gevent.spawn(race)
    self.mock_ss_provider.AddServer('localhost', 8080)
    racer.join()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)

if __name__ == '__main__':
  unittest.main()
