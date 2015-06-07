import sys

import gevent
from gevent.event import AsyncResult

def WhenAll(ars):
  ret = AsyncResult()
  total = [len(ars)]
  results = [None] * len(ars)
  def complete(_ar, _n):
    if _ar.exception:
      ret.set_exception(_ar.exception)
    total[0] -= 1
    results[_n] = _ar.value
    if total[0] == 0:
      ret.set(results)

  for n, ar in enumerate(ars):
    ar.rawlink(lambda a: complete(a, n))
  return ret

def WhenAny(ars):
  ret = AsyncResult()
  total = [len(ars)]
  def complete(_ar):
    total[0] -= 1
    if total[0] == 0 and _ar.exception:
      ret.set_exception(_ar.exception)
    elif _ar.successful():
      ret.set(_ar.value)

  for ar in ars:
    ar.rawlink(complete)
  return ret

def Complete():
  ret = AsyncResult()
  ret.set()
  return ret

def _SafeLinkHelper(ar, fn):
  try:
    ret = fn()
    ar.set(ret)
  except:
    ar.set_exception(sys.exc_info()[0])

def SafeLink(ar, fn):
  gevent.spawn(_SafeLinkHelper, ar, fn)
