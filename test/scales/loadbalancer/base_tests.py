from scales.loadbalancer import ApertureBalancerSink
from test.scales.util.mocks import (MockServerSetProvider)
from test.scales.util.base import SinkTestCase

class LoadBalancerTestCase(SinkTestCase):
  def _getLoadedNode(self):
    return next(n for n in self.sink._heap[1:] if n.load > self.sink.Idle)

  def customize(self):
    ss_provider = MockServerSetProvider()
    for p in (8080, 8081, 8082):
      ss_provider.AddServer('localhost', p)
    self.mock_ss_provider = ss_provider
    self.global_properties.update({
      'open_delay': self._open_delay })

    props = ApertureBalancerSink.Builder._defaults.copy()
    props['server_set_provider'] = ss_provider
    self.sink_properties = ApertureBalancerSink.Builder.PARAMS_CLASS(**props)


