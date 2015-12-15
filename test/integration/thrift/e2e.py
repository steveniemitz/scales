from scales.thriftmux import ThriftMux
from test.integration.thrift.gen_py.example_rpc_service import ExampleService, ttypes

class MessageHandler(object):
  def passMessage(self, message):
    return ttypes.ServiceResponse('%s boom' % message.content)

def server():
  from scales.thriftmux.server_sink import ThriftMuxServerSocketSink, ServerCallBuilderSink
  from scales.thriftmux.sink import ThriftMuxServerMessageSerializerSink
  from scales.scales_socket import ScalesSocket
  sock = ScalesSocket('0.0.0.0', 8081)

  serializer_provider = ThriftMuxServerMessageSerializerSink.Builder()
  serializer_provider.next_provider = ServerCallBuilderSink.Builder(handler=MessageHandler())
  s = ThriftMuxServerSocketSink(sock, ExampleService.Iface, serializer_provider)
  s.Open()

  from gevent.event import Event
  Event().wait()

def client():
  c = ThriftMux.NewClient(ExampleService.Iface, "tcp://127.0.0.1:8081", 0)
  ret = c.passMessage(ExampleService.Message("test data"))
  print 'Responded with %s' % ret

if __name__ == '__main__':
  import logging
  logging.basicConfig(level=logging.DEBUG)

  import sys
  if sys.argv[1] == 'server':
    server()
  else:
    client()