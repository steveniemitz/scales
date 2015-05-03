__author__ = 'me'
import time
import traceback

from src.dispatch import MessageDispatcher
from src.pynagle import TMuxTransport
from thrift.protocol import TBinaryProtocol
from thrift.transport import (TSocket, TTransport)
from gen_py.hello.Hello import Client

import gevent
from gevent import monkey
monkey.patch_all(thread=False)

if __name__ == '__main__':
  def fn():
    sock = TSocket.TSocket("localhost", 8080)
    dispatcher = MessageDispatcher(sock)
    buf = TMuxTransport(dispatcher)
    prot = TBinaryProtocol.TBinaryProtocolAccelerated(buf)

    client = Client(prot)
    buf.open()

    num_iters = 2
    start_time = time.time()
    for i in range(0, num_iters):
      try:
        client.hi()
      except Exception:
        traceback.print_exc()

    end_time = time.time()
    dur = end_time - start_time
    print "Duration: %f, calls / sec = %f" % (dur, num_iters / dur)

    gevent.sleep(3)

    buf.close()

  fn()