from scales.thriftmux import ThriftMux
from test.integration.thrift.gen_py.example_rpc_service import ExampleService, ttypes

class MessageHandler(object):
  def passMessage(self, message):
    return ttypes.ServiceResponse('%s boom' % message.content)

def server():
  from scales.thriftmux import ThriftMux
  from scales.loadbalancer.zookeeper import Endpoint
  sink = ThriftMux.NewServerBuilder(ExampleService.Iface, MessageHandler(), Endpoint('0.0.0.0', 8081))
  sink.Open()
  sink.Wait()

def client():
  c = ThriftMux.NewClient(ExampleService.Iface, "tcp://127.0.0.1:8081", 10)
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