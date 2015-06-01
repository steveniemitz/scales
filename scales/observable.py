import gevent

class Observable(object):
  __slots__ = ('_callbacks', '_value')

  def __init__(self):
    self._callbacks = set()
    self._value = None

  def __Notify(self, value):
    cb_copy = self._callbacks.copy()
    [cb(value) for cb in cb_copy]

  def Get(self):
    return self._value

  def Set(self, value):
    self._value = value
    gevent.spawn(self.__Notify, value)

  def Subscribe(self, callback):
    self._callbacks.add(callback)

  def Unsubscribe(self, callback):
    self._callbacks.discard(callback)
