from thrift.transport import TSocket

from .sink import (
    ThriftMuxMessageSerializerSink,
    SocketTransportSink,
    TimeoutSink
)

from ..builder import BaseBuilder
from ..channel_resurrector import ResurrectorChannelSinkProvider
from ..loadbalancer import ApertureBalancerChannelSink
from ..pool import SingletonPoolChannelSinkProvider
from ..sink import (
  TransportSinkStackBuilder,
  MessageSinkStackBuilder
)
from ..varz import VarzSocketWrapper


class ThriftMux(BaseBuilder):
  """A builder class for building clients to ThriftMux services."""
  class MessageSinkStackBuilder(MessageSinkStackBuilder):
    def CreateSinkStack(self, builder):
      name = builder.name

      pool_provider = SingletonPoolChannelSinkProvider(ThriftMux.TransportSinkStackBuilder())
      resurrector = ResurrectorChannelSinkProvider(pool_provider)
      balancer = ApertureBalancerChannelSink(resurrector, name, builder.server_set_provider)

      prev_sink = TimeoutSink(name)
      head_sink = prev_sink
      for sink in [ThriftMuxMessageSerializerSink(name), balancer]:
        prev_sink.next_sink = sink
        prev_sink = sink
      return head_sink

  class TransportSinkStackBuilder(TransportSinkStackBuilder):
    def _CreateSocket(self, host, port):
      return TSocket.TSocket(host, port)

    def CreateSink(self, server, pool_name):
      sock = self._CreateSocket(server.host, server.port)
      healthy_sock = VarzSocketWrapper(sock, pool_name)
      sink = SocketTransportSink(healthy_sock, pool_name)
      return sink
