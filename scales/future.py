import gevent
from gevent.event import Event


class Future(object):
  __slots__ = ('_result', '_ex', '_cbs', '_event')

  _CB_Success = 1
  _CB_Failure = 2
  _CB_Always = 3

  def __init__(self):
    self._result = None
    self._ex = None
    self._cbs = None
    self._event = None

  def _HookUp(self, cb_type, fn, args, kwargs):
    if self._result or self._ex:
      fn(self, *args, **kwargs)
      return

    if not self._cbs:
      self._cbs = []
    f = GreenletFuture(fn)
    self._cbs.append((cb_type, f, args, kwargs))
    return f

  def __call__(self, fn, *args, **kwargs):
    return self.Success(fn, args, kwargs)

  def Success(self, fn, *args, **kwargs):
    self._HookUp(self._CB_Success, fn, args, kwargs)
    return self

  def Failure(self, fn, *args, **kwargs):
    self._HookUp(self._CB_Failure, fn, args, kwargs)
    return self

  def Always(self, fn, *args, **kwargs):
    self._HookUp(self._CB_Always, fn, args, kwargs)
    return self

  def Then(self, fn, *args, **kwargs):
    return self._HookUp(self._CB_Success, fn, args, kwargs)

  def ThenAlways(self, fn, *args, **kwargs):
    return self._HookUp(self._CB_Always, fn, args, kwargs)

  def Map(self, fn, *args, **kwargs):
    def mapper(f):
      if f.is_faulted:
        return f
      else:
        return fn(f.result, *args, **kwargs)
    return self._HookUp(self._CB_Always, mapper, args, kwargs)

  def SetResult(self, result):
    if not self._result or self._ex:
      self._result = result
      self._OnResult(result, None)

  def SetException(self, ex):
    if not self._result or self._ex:
      self._ex = ex
      self._OnResult(None, ex)

  def Wait(self, timeout=None):
    if self._result or self._ex:
      return

    if not self._event:
      self._event = Event()
    self._event.wait(timeout)

  def _SpawnCallbacks(self, cb_type):
    if self._cbs:
      [cb[1].Start(self, *cb[2], **cb[3]) for cb in self._cbs
       if cb[0] == cb_type or cb[0] == self._CB_Always]

  @property
  def result(self):
    if self._result:
      return self._result
    elif self._ex:
      raise self._ex

  @property
  def exception(self):
    return self._ex

  @property
  def is_faulted(self):
    return self._ex is not None

  def _OnResult(self, result, ex):
    if self._event:
      self._event.set()

    if result:
      self._SpawnCallbacks(self._CB_Success)
    elif ex:
      self._SpawnCallbacks(self._CB_Failure)

  @staticmethod
  def StartNew(fn):
    f = GreenletFuture(fn)
    f.Start()
    return f

  @staticmethod
  def FromResult(r):
    f = Future()
    f.SetResult(r)
    return f

  @staticmethod
  def WhenAll(futures):
    composite = Future()
    total = [0]
    def complete(fr):
      total[0] -= 1
      if fr.is_faulted:
        composite.SetException(fr.exception)
        return
      if total[0] == 0:
        composite.SetResult(futures)

    for f in futures:
      f.Always(complete)
      total[0] += 1

    return composite

class GreenletFuture(Future):
  __slots__ = '_greenlet', '_fn'

  def __init__(self, fn):
    super(GreenletFuture, self).__init__()
    self._fn = fn
    self._greenlet = None

  def Start(self, *args, **kwargs):
    def _OnExit(gr):
      if gr.exception:
        self.SetException(gr.exception)
      else:
        self.SetResult(gr.value)
    self._greenlet = gevent.spawn(self._fn, *args, **kwargs).link(_OnExit)

  def Cancel(self):
    self._greenlet.kill()
