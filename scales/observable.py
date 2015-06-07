import gevent

class Observable(object):
  __slots__ = ('_callbacks', '_one_shot_callbacks', '_value')

  def __init__(self):
    self._callbacks = None
    self._one_shot_callbacks = None
    self._value = None

  def __Notify(self, value):
    if self._callbacks:
      cb_copy = self._callbacks.copy()
      [cb(value) for cb in cb_copy]
    if self._one_shot_callbacks:
      cb_copy, self._one_shot_callbacks = self._one_shot_callbacks, None
      [cb(value) for cb in cb_copy]

  def Get(self):
    return self._value

  def Set(self, value=None):
    self._value = value
    gevent.spawn(self.__Notify, value)

  def Subscribe(self, callback, one_shot=False):
    if callback is None:
      raise Exception("callback can not be None")

    if one_shot:
      if not self._one_shot_callbacks:
        self._one_shot_callbacks = set()
      self._one_shot_callbacks.add(callback)
    else:
      if not self._callbacks:
        self._callbacks = set()
      self._callbacks.add(callback)

  def Unsubscribe(self, callback):
    if self._callbacks:
      self._callbacks.discard(callback)
    if self._one_shot_callbacks:
      self._one_shot_callbacks.discard(callback)
