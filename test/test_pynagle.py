from __future__ import print_function

from collections import defaultdict, Counter

import logging
import random
import traceback

import gevent
from gevent import monkey
from gevent.event import Event

from scales.core import Scales
from scales.thriftmux import ThriftMux
from scales.pool.singleton import SingletonPoolSink
from scales.thrift import Thrift
from scales.varz import (SourceType, VarzReceiver)

from scales.timer_queue import TimerQueue

from test.gen_py.example_rpc_service import ExampleService
from test.gen_py.example_rpc_service import ttypes
monkey.patch_all(thread=False)

class ServiceMetric(object):
  def __init__(self):
    self.success = 0
    self.total = 0
    self.failed = 0
    self.pct_successful = 0
    self.latency_avg = 0
    self.bytes_recv = 0
    self.bytes_sent = 0
    self.pool_size = 0
    self.pool_healthy = 0
    self.pool_unhealthy = 0
    self.request_latency_50 = 0.0
    self.request_latency_90 = 0.0
    self.request_latency_95 = 0.0
    self.request_latency_99 = 0.0
    self.request_latency_num = 0

last_dct = defaultdict(int)

def dump_greenlets():
  #stats = GreenletProfiler.get_func_stats()
  #stats.sort('tsub')
  #stats.print_all()
  #return

  global last_dct
  import gc
  import traceback
  from greenlet import greenlet

  gc.collect()
  objs = gc.get_objects()

  dct = defaultdict(int)

  for o in objs:
    dct[type(o)] += 1

  print("------------ %d objects -----------" % len(gc.get_objects()))
  for k,v in sorted(dct.items(), key=lambda i: -i[1]):
    print('%40s -> %d [%d]' % (k.__name__, v, v - last_dct[k]))

  last_dct = dct
  for ob in gc.get_objects():
    if isinstance(ob, SingletonPoolSink):
      print('Found sink %d' % id(ob))

  for ob in gc.get_objects():
    if not isinstance(ob, greenlet):
      continue
    if not ob:
      continue
    #print("------------------")
    #print(''.join(traceback.format_stack(ob.gr_frame)))

if __name__ == '__main__':
  logging.basicConfig(level='DEBUG')

  def dumpVarz(varz=VarzReceiver.VARZ_DATA):
    print('-----------------------')
    for metric in sorted(varz.keys()):
      metric_info = VarzReceiver.VARZ_METRICS[metric]
      services = varz[metric]
      print('%s: ' % str(metric))
      for source in sorted(services.keys()):
        unpacked_sources = []
        orig_source = source
        source = list(source)
        for st in (SourceType.Method, SourceType.Service, SourceType.Endpoint):
          if metric_info[1] & st != 0:
            unpacked_sources.append(source.pop(0))
          else:
            unpacked_sources.append(None)

        method, service, endpoint = unpacked_sources
        print(' - %s, %s, %s = %s' % (method, service, endpoint, str(services[orig_source])))

  def aggVarz():
    aggregated_varz = defaultdict(ServiceMetric)
  varz = VarzReceiver.VARZ_DATA
  for metric in varz.keys():
    prop = VAR_TO_PROPERTY.get(metric)
    if not prop:
      continue

    pct_props = (prop + '_50', prop + '_90', prop + '_95', prop + '_99')
    metric_info = VarzReceiver.VARZ_METRICS[metric]
    services = varz[metric]
    for source in services.keys():
      unpacked_sources = []
      orig_source = source
      source = list(source)
      for st in (SourceType.Method, SourceType.Service, SourceType.Endpoint):
        if metric_info[1] & st != 0:
          unpacked_sources.append(source.pop(0))
        else:
          unpacked_sources.append(None)

      method, service, endpoint = unpacked_sources

      agg = aggregated_varz[service]
      value = services[orig_source]
      if metric_info[0] == VarzType.AverageTimer:
        setattr(agg, prop + '_num', getattr(agg, prop + '_num') + 1)
        for idx, p in enumerate(pct_props):
          current_val = getattr(agg, prop)
          current_val += value[idx]
          setattr(agg, prop, current_val)
      else:
        current_val = getattr(agg, prop)
        current_val += value
        setattr(agg, prop, current_val)

    for agg in aggregated_varz[metric].values():
      if agg.total != 0:
        agg.pct_successful = agg.success / agg.total
      if metric_info[0] == VarzType.AverageTimer:
        n = getattr(agg, prop + '_num')
        if n != 0:
          for m in pct_props:
            setattr(agg, m, getattr(agg, m) / n)

  def test_timer_queue():
    tq = TimerQueue()
    import time
    start_time = time.time()
    def print_it(n):
      print('%f: Waited %f' % (n, time.time() - start_time))

    c = tq.Schedule(.26, print_it, .26)
    tq.Schedule(.15, print_it, .15)
    tq.Schedule(.22, print_it, .22)
    tq.Schedule(.17, print_it, .17)
    tq.Schedule(.2, print_it, .2)
    tq.Schedule(.24, print_it, .24)
    c()

    print(tq._queue)
    gevent.sleep(1)

  #test_timer_queue()

  def fn():
    from gen_py.scribe import scribe
    from gen_py.scribe.ttypes import LogEntry
    from gen_py.hello import Hello

    client = Thrift.NewClient(Hello.Iface, 'tcp://localhost:8080')
    ret = client.hi('hi!')
    ret = client.hi('hi!')
    ret = client.hi('hi!')
    print(ret)
    client.DispatcherClose()


    client = ThriftMux.NewClient(Hello.Iface, 'tcp://localhost:8080')
    ret = client.hi('hi!')
    ret = client.hi('hi!')
    ret = client.hi('hi!')
    print(ret)

    p = Scales.SERVICE_REGISTRY[Hello.Iface][0]
    while p:
      print(p.sink_class)
      p = p.next_provider

    dumpVarz()
    return
    #return
    #client = Thrift.newClient(scribe.Iface, 'tcp://ec2-54-161-50-43.compute-1.amazonaws.com:15740')
    #client = Thrift.NewClient(scribe.Iface, 'zk://zk.aue1.tellapart.net:2181/service_discovery/aue1/scribe/devel/agent')
    #ret = client.Log([LogEntry('test', 'steve is cool')])
    #print(ret)
    #dumpVarz()
    #return
    #print(ret)
    #client = ThriftMux.newClient(ExampleService.Iface, 'tcp://localhost:8080')
    #ret = client.passMessage(ttypes.Message('hi!'))
    #ret = client.passMessage(ttypes.Message('hi!'))
    #dumpVarz()
    #gevent.sleep(1)
    #dump_greenlets()
    #return
    #print(ret)

    #ret = client.hi_async('test')
    #print(ret.get())
    def fn2(n):
      x = 0
      while True:
        x+=1
        try:
          client.hi('hi!')
          #client.Log([LogEntry('test', 'steve is cool')])
          #client.passMessage(ttypes.Message('hi!'))
        except:
          traceback.print_exc()
          pass
        gevent.sleep(random.random() / 10)

    for n in range(10):
      gevent.spawn(fn2, n)

    e = Event()
    while True:
      e.wait(10)
      #aggVarz()
      dumpVarz()
      #dump_greenlets()
      #dumpVarz(VarzReceiver.VARZ_DATA_PERCENTILES)

  fn()
