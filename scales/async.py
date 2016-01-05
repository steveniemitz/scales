"""Utility methods for manipulating AsyncResults."""

import functools
import sys

import gevent
from gevent import Greenlet
from gevent.event import AsyncResult as g_AsyncResult

class NamedGreenlet(Greenlet):
  def __init__(self, run=None, *args, **kwargs):
    self.name = None
    Greenlet.__init__(self, run, *args, **kwargs)

  @classmethod
  def spawn(cls, name, *args, **kwargs):
    g = cls(*args, **kwargs)
    g.name = name
    g.start()
    return g

  def __repr__(self):
    if self.name:
      return self.name
    else:
      return NamedGreenlet.__repr__(self)

class AsyncResult(g_AsyncResult):
  @staticmethod
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
    def complete(_n, _ar):
      if _ar.exception:
        ret.set_exception(_ar.exception)
      elif not ret.ready():
        total[0] -= 1
        results[_n] = _ar.value
        if total[0] == 0:
          ret.set(results)

    for n, ar in enumerate(ars):
      ar.rawlink(functools.partial(complete, n))
    return ret

  @staticmethod
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
    if ready_ars:
      return ready_ars[0]

    ret = AsyncResult()
    total = [len(ars)]
    def complete(_ar):
      total[0] -= 1
      if total[0] == 0 and _ar.exception:
        ret.set_exception(_ar.exception)
      elif not ret.ready() and _ar.successful():
        ret.set(_ar.value)

    for ar in ars:
      ar.rawlink(complete)
    return ret

  @staticmethod
  def FromValue(val):
    if val is None:
      return AsyncResult.Complete()
    else:
      ar = AsyncResult()
      ar.set(val)
      return ar

  @staticmethod
  def Complete():
    """Return an AsyncResult that has completed."""
    return _COMPLETE

  @staticmethod
  def CompleteIn(n):
    """Returns an AsyncResult that completes in <n> seconds

    Args:
      n - The number of seconds to wait before completing.
    """
    ar = AsyncResult()
    def helper():
      ar.set()
    g = Greenlet(helper)
    g.start_later(float(n))
    return ar

  def _SafeLinkHelper(self, fn):
    try:
      self.set(fn())
    except:
      self.set_exception(sys.exc_info()[1])

  def SafeLink(self, fn):
    """Propagate the result of calling fn() on a new greenlet to ar

    Args:
      ar - An AsyncResult.
      fn - The function to execute.
    """
    gevent.spawn(self._SafeLinkHelper, fn)

  def ContinueWith(self, fn, on_hub=True):
    cw_ar = AsyncResult()
    def continue_with_callback(_ar):
      def run():
        try:
          val = fn(_ar)
          cw_ar.set(val)
        except:
          cw_ar.set_exception(sys.exc_info()[1])
      if on_hub:
        run()
      else:
        gevent.spawn(run)
    self.rawlink(continue_with_callback)
    return cw_ar

  def Map(self, fn):
    def mapper(_):
      if self.exception:
        return self
      else:
        return fn(self.value)
    return self.ContinueWith(mapper).Unwrap()

  def _UnwrapHelper(self, target):
    if self.ready():
      # We're ready, propagate the result
      if self.exception:
        target.set_exception(self.exception)
      else:
        if isinstance(self.value, AsyncResult):
          self.value._UnwrapHelper(target)
        else:
          target.set(self.value)
    else:
      self.rawlink(
        functools.partial(AsyncResult._UnwrapHelper, target=target))

  def Unwrap(self):
    unwrapped_ar = AsyncResult()
    self._UnwrapHelper(unwrapped_ar)
    return unwrapped_ar

  @staticmethod
  def TryGet(val):
    if isinstance(val, AsyncResult):
      return val.get()
    else:
      return val

  @staticmethod
  def Run(fn):
    ar = AsyncResult()
    ar.SafeLink(fn)
    return ar

  @staticmethod
  def RunInline(fn):
    ar = AsyncResult()
    ar._SafeLinkHelper(fn)
    return ar

_COMPLETE = AsyncResult()
_COMPLETE.set()

class NoopTimeout(object):
  def start(self): pass
  def cancel(self): pass
