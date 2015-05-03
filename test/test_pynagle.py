__author__ = 'me'

from src.dispatch import MessageDispatcher
from src.pynagle import TMuxTransport
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from gen_py.hello.Hello import Client

import gevent
from gevent import monkey
monkey.patch_all(thread=False)

if __name__ == '__main__':
  def fn():
    dispatcher = MessageDispatcher(TSocket.TSocket("localhost", 8080))
    buf = TMuxTransport(dispatcher)
    prot = TBinaryProtocol.TBinaryProtocolAccelerated(buf)

    client = Client(prot)
    buf.open()

    print(client.hi())
    print(client.hi())
    print(client.hi())
    buf.close()

    while True:
      gevent.sleep(100)

  fn()