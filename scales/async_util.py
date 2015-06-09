"""Utility methods for manipulating AsyncResults."""

import sys

import gevent
from gevent.event import AsyncResult

def WhenAll(ars):
  """Returns an AsyncResult representing the state of all AsyncResults passed.

  Args:
    ars - An enumerable of AsyncResults.
  Returns:
    An AsyncResult representing the completion of all ars passed in.  When all
    complete, the AsyncResult will be set to an array of the results of each
    AsyncResult, in the order they were enumerated in.
    If any AsyncResult fails, the return result will fail.
  """

  ret = AsyncResult()
  num_ars = len(ars)
  total = [num_ars]
  results = [None] * num_ars
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
  """Returns an AsyncResult representing the state of any AsyncResult passed in.
  The return value represents the state of the first AsyncResult to complete, or,
  if all fail, the last to fail.

  Args:
    ars - An enumerable of AsyncResults.
  Returns:
    An AsyncResult representing the state of the first AsyncResult to complete.
    The AsyncResult's value will be set to the value of the first result to
    complete, or, if all fail, the exception thrown by the last to fail.
  """
  ready_ars = [ar for ar in ars if ar.ready()]
  if any(ready_ars):
    return ready_ars[0]

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

_COMPLETE = AsyncResult()
_COMPLETE.set()

def Complete():
  """Return an AsyncResult that has completed."""
  return _COMPLETE

def _SafeLinkHelper(ar, fn):
  try:
    ret = fn()
    ar.set(ret)
  except:
    ar.set_exception(sys.exc_info()[0])

def SafeLink(ar, fn):
  """Propagate the result of calling fn() on a new greenlet to ar

  Args:
    ar - An AsyncResult.
    fn - The function to execute.
  """
  gevent.spawn(_SafeLinkHelper, ar, fn)
