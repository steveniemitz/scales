import time
import unittest

import gevent

from scales.timer_queue import TimerQueue

class TimerQueueTestCase(unittest.TestCase):
  def setUp(self):
    self.tq = TimerQueue(resolution=0)

  def testTimerQueueBasic(self):
    now = time.time()
    orders = []
    self.tq.Schedule(now+.1, lambda : orders.append(1))
    self.tq.Schedule(now+.2, lambda : orders.append(2))
    self.tq.Schedule(now+.3, lambda : orders.append(3))
    gevent.sleep(.5)
    self.assertEqual(orders, [1,2,3])

  def testTimerQueueOutOfOrder(self):
    now = time.time()
    orders = []
    self.tq.Schedule(now+.1, lambda : orders.append(1))
    self.tq.Schedule(now+.3, lambda : orders.append(3))
    gevent.sleep(.15)
    self.tq.Schedule(now+.2, lambda : orders.append(2))
    gevent.sleep(.5)
    self.assertEqual(orders, [1,2,3])

  def testTimerQueueNewHead(self):
    now = time.time()
    orders = []
    self.tq.Schedule(now+.3, lambda : orders.append(3))
    self.tq.Schedule(now+.2, lambda : orders.append(2))
    gevent.sleep(.1)
    self.tq.Schedule(now+.15, lambda : orders.append(1))
    gevent.sleep(.5)
    self.assertEqual(orders, [1,2,3])

if __name__ == '__main__':
  unittest.main()
