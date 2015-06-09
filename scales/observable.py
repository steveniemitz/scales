import gevent

class Observable(object):
  """An object to which consumers can subscribe to notifications for its value
  changing."""
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
    """Get the current value of the Observable."""
    return self._value

  def Set(self, value=None):
    """Set the current value of the Observable and notify any subscribers."""
    self._value = value
    gevent.spawn(self.__Notify, value)

  def Subscribe(self, callback, one_shot=False):
    """Subscribe to notifications of the value changing.

    Args:
      callback - The method to be called when the value changes.
      one_shot - If true, the callback will only be invoked once.
    """
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
    """Unsubscribe from notifications.

    Args:
      callback - The method previously subscribed.
    """
    if self._callbacks:
      self._callbacks.discard(callback)
    if self._one_shot_callbacks:
      self._one_shot_callbacks.discard(callback)
