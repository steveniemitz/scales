import unittest

import gevent

from scales.asynchronous import AsyncResult

class AsyncUtilTestCase(unittest.TestCase):
  def testWhenAllSuccessful(self):
    ar1 = AsyncResult()
    ar2 = AsyncResult()

    ar_agg = AsyncResult.WhenAll((ar1, ar2))
    self.assertEqual(ar_agg.ready(), False)

    ar1.set(1)
    gevent.sleep(0)
    self.assertEqual(ar_agg.ready(), False)
    ar2.set(2)
    gevent.sleep(0)
    self.assertEqual(ar_agg.ready(), True)
    self.assertEqual(ar_agg.value, [1,2])

  def testWhenAnySuccessful(self):
    ar1 = AsyncResult()
    ar2 = AsyncResult()

    ar_agg = AsyncResult.WhenAny((ar1, ar2))
    self.assertEqual(ar_agg.ready(), False)

    ar1.set('hi')
    gevent.sleep(0)
    self.assertEqual(ar_agg.value, 'hi')

  def testWhenAnyFailure(self):
    ar1 = AsyncResult()
    ar2 = AsyncResult()

    ar_agg = AsyncResult.WhenAny((ar1, ar2))
    ar1.set_exception(Exception())
    gevent.sleep(0)
    ar2.set(1)
    gevent.sleep(0)
    self.assertEqual(ar_agg.value, 1)

  def testWhenAllFailure(self):
    ar1 = AsyncResult()
    ar2 = AsyncResult()

    ar_agg = AsyncResult.WhenAll((ar1, ar2))
    ar1.set(1)
    gevent.sleep(0)
    ar2.set_exception(Exception())
    gevent.sleep(0)
    self.assertIsNotNone(ar_agg.exception)

  def testUnwrapAlreadyDone(self):
    ar_outer = AsyncResult()
    ar_inner = AsyncResult()

    ar_outer.set(ar_inner)
    ar_inner.set('test')
    ar_unwrapped = ar_outer.Unwrap()
    self.assertEqual(ar_unwrapped.get(), 'test')

  def testUnwrapLater(self):
    ar_outer = AsyncResult()
    ar_inner = AsyncResult()

    def set_ar_inner():
      gevent.sleep(.01)
      ar_outer.set(ar_inner)
      gevent.sleep(.01)
      ar_inner.set('test')
    gevent.spawn(set_ar_inner)

    ar_unwrapped = ar_outer.Unwrap()
    self.assertFalse(ar_unwrapped.ready())
    self.assertEqual(ar_unwrapped.get(), 'test')


if __name__ == '__main__':
  unittest.main()
