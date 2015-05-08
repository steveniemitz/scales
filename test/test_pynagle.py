import collections
import functools
import time
import traceback

from src.thriftmux import ThriftMux
from src.thealthysocket import _VARZ_DATA

import gevent
from gevent import monkey
monkey.patch_all(thread=False)


if __name__ == '__main__':
  def fn():
    from gen_py.hello import Hello
    client = ThriftMux.newService(Hello.Client, 'localhost:8080')

    def fn2(n):
      for x in range(20):
        print '%d %s' % (n, client.hi('from %d test %s' % (n, x)))

    gevent.spawn(fn2, 1)
    gevent.spawn(fn2, 2)

    gevent.sleep(2)

    import pprint
    pprint.pprint(_VARZ_DATA)


  fn()
