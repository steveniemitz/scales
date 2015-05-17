from __future__ import print_function

import logging
import random
import traceback

import gevent
from gevent import monkey
from gevent.event import Event

from scales.thriftmux import ThriftMux
from scales.varz import VarzReceiver

monkey.patch_all(thread=False)

if __name__ == '__main__':
  logging.basicConfig(level='DEBUG')
  def fn():
    from gen_py.hello import Hello
    client = ThriftMux.newClient(Hello.Iface, 'tcp://localhost:8080')
    #ret = client.hi_async('test')
    #print(ret.get())
    def fn2(n):
      x = 0
      while True:
        x+=1
        try:
          client.hi('test')
        except:
          traceback.print_exc()
        gevent.sleep(random.random() / 10)

    gevent.spawn(fn2, 1)
    gevent.spawn(fn2, 2)

    e = Event()
    while True:
      varz = VarzReceiver.VARZ_DATA
      for k in sorted(varz.keys()):
        print('%s = ' % str(k), end='')
        print(varz[k])
      e.wait(10)

  fn()
