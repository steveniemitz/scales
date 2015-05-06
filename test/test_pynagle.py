import collections
import functools
import time
import traceback

from src.dispatch import MuxMessageDispatcher
from src.thriftmux import TMuxTransport
from src.pool import _RoundRobinSingletonPool
from src.thealthysocket import (THealthySocket, _VARZ_DATA)
from thrift.protocol import TBinaryProtocol
from thrift.transport import (TSocket, TTransport)
from gen_py.hello.Hello import Client

import gevent
from gevent import monkey
monkey.patch_all(thread=False)

Endpoint = collections.namedtuple('Endpoint', 'host port')
Server = collections.namedtuple('Server', 'service_endpoint')

class MuxDispatcherProvider(object):
  def GetConnection(self, server, health_cb, timeout):
    sock = THealthySocket(server.host, server.port, None, None, 'test')
    disp = MuxMessageDispatcher(sock, timeout)
    disp.shutdown_result.rawlink(lambda ar: health_cb(server))
    return disp

  def IsConnectionFault(self, e):
    return isinstance(e,  TTransport.TTransportException)

class StaticServerSet(object):
  def __init__(self, servers):
    self._servers = servers

  def __iter__(self):
    return self._servers.__iter__()

def StaticServerSetProvider(servers, on_join, on_leave):
  return StaticServerSet(servers)


if __name__ == '__main__':
  def fn():
    server = Server(Endpoint('localhost', 8080))
    pool = _RoundRobinSingletonPool(
        'test',
        functools.partial(StaticServerSetProvider, servers=[server]),
        MuxDispatcherProvider(),
        no_pop=True
    )
    buf = TMuxTransport(pool)
    prot = TBinaryProtocol.TBinaryProtocolAccelerated(buf)

    client = Client(prot)
    buf.open()

    num_iters = 20
    start_time = time.time()
    for i in range(0, num_iters):
      try:
        client.hi()
        gevent.sleep(.1)
      except Exception:
        traceback.print_exc()

    end_time = time.time()
    dur = end_time - start_time
    print "Duration: %f, calls / sec = %f" % (dur, num_iters / dur)

    gevent.sleep(1)
    import pprint
    pprint.pprint(_VARZ_DATA)

    buf.close()

  fn()
