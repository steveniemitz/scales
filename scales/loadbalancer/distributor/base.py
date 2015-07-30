from ...loadbalancer.base import LoadBalancerSink
from ...loadbalancer.serverset import StaticServerSetProvider
from ...constants import (
  SinkRole
)

class DistributorSink(LoadBalancerSink):
  Role = SinkRole.LoadBalancer

  class ChildServerSetProvider(StaticServerSetProvider):
    def __init__(self):
      self._on_join = None
      self._on_leave = None
      super(DistributorSink.ChildServerSetProvider, self).__init__(set())

    def Initialize(self, on_join, on_leave):
      self._on_join = on_join
      self._on_leave = on_leave

    def AddServer(self, server):
      self._servers.add(server)
      if self._on_join:
        self._on_join(server)

    def RemoveServer(self, server):
      self._servers.discard(server)
      if self._on_leave:
        self._on_leave(server)

    def GetServers(self):
      return list(self._servers)

  def __init__(self, next_provider, sink_properties, global_properties):
    super(DistributorSink, self).__init__(next_provider, sink_properties, global_properties)
