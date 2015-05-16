import random
from scales.thriftmux import ThriftMux
from scales.varzsocketwrapper import VARZ_DATA

import gevent
from gevent import monkey
from gevent.event import Event
monkey.patch_all(thread=False)

if __name__ == '__main__':
  def fn():
    from gen_py.hello import Hello
    client = ThriftMux.newClient(Hello.Client, 'tcp://localhost:8080')
    client2 = ThriftMux.newClient(Hello.Client, 'tcp://localhost:8080')
    client.hi('test')
    def fn2(n):
      x = 0
      while True:
        x+=1
        print '%d %s' % (n, client.hi('from %d test %s' % (n, x)))
        #gevent.sleep(random.random())

    gevent.spawn(fn2, 1)
    gevent.spawn(fn2, 2)

    e = Event()
    e.wait(5)

    import pprint
    pprint.pprint(VARZ_DATA)


  fn()
