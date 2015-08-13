from __future__ import absolute_import

import unittest

from scales.constants import ChannelState
from scales.loadbalancer import ApertureBalancerSink
from test.scales.loadbalancer.base_tests import LoadBalancerTestCase
from test.scales.util.mocks import MockSinkProvider

class ApertureBalancerTestCase(LoadBalancerTestCase):
  SINK_CLS = ApertureBalancerSink

  def testApertureInitialSize(self):
    self.assertEqual(self.sink._min_size, self.sink._size)

  def testApertureOpensOnlyMinSizeNodes(self):
    min_size = self.sink._min_size
    sinks_open = sum(1 if s.state == ChannelState.Open else 0
                     for s in self.mock_provider.sinks_created)
    self.assertEqual(
        len(self.mock_provider.sinks_created),
        min_size)
    self.assertEqual(min_size, sinks_open)

  def testApertureExpandsOnDownedNode(self):
    active_aperture_node = self.sink._heap[1].channel
    active_aperture_node.state = ChannelState.Closed
    self._submitTestMessage()
    self._completeTestMessage()
    self.assertEqual(self.return_message, self.MSG_SENTINEL)

  def testApertureHandlesInitialNodeDown(self):
    self.mock_provider = MockSinkProvider()
    self.global_properties['num_failures'] = [1]
    self.sink = self._createSink()
    ar = self.sink.Open()
    ar.wait()

    # The open should succeed
    self.assertTrue(ar.successful)
    # It should NOT return an AsyncResult<AsyncResult<...>>
    # (eg unwrapping should work)
    self.assertNotIsInstance(ar.value, ar.__class__)
    # Two sinks should now be active, the failed one and the successful one
    self.assertEqual(self.sink._size, 2)
    # Only one should be open (since one failed)
    self.assertEqual(len([n for n in self.mock_provider.sinks_created
                          if n.state == ChannelState.Open]), 1)


if __name__ == '__main__':
  unittest.main()
