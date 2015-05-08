from __future__ import absolute_import

import collections
import functools

from thrift.transport import (TSocket, TTransport)

def wrap_exceptions(exception_callback):
  def wrapper(fn):
    @functools.wraps(fn)
    def _wrapper(self, *args, **kwargs):
      try:
        return fn(self, *args, **kwargs)
      except Exception as e:
        exception_callback(self, e)
        raise
    return _wrapper
  return wrapper

_VARZ_DATA = collections.defaultdict(int)
_VARZ_PREFIX = 'scales'

class THealthySocket(TSocket.TSocket):
  def __init__(self, host, port, unix_socket, health_event_cb, varz_tag=None):
    def noop(*args, **kwargs): pass
    self._health_event_cb = health_event_cb or noop
    self._write_varz = varz_tag is not None
    if self._write_varz:
      self._InitVarz(varz_tag)
    else:
      self._varz_connections = None
      self._varz_bytes_recv = None
      self._varz_bytes_sent = None

    TSocket.TSocket.__init__(self, host, port, unix_socket)

  def _InitVarz(self, varz_tag):
    self._varz_bytes_recv = '_'.join([_VARZ_PREFIX, varz_tag, 'bytes_recv'])
    self._varz_bytes_sent = '_'.join([_VARZ_PREFIX, varz_tag, 'bytes_sent'])
    self._varz_connections = '_'.join([_VARZ_PREFIX, varz_tag, 'num_connections'])

  def _HealthCallback(self, ex):
    # Only pass through failures if it's from the connection being closed.
    if ((ex is TTransport.TTransportException
         and ex.type == TTransport.TTransportException.NOT_OPEN)
        or not self.isOpen()):
      self._health_event_cb(self, ex)

  def _IncrementVarz(self, metric, amount):
    if not self._write_varz:
      return
    _VARZ_DATA[metric] += amount

  wrap_health_exceptions = functools.partial(wrap_exceptions, _HealthCallback)

  @wrap_health_exceptions()
  def read(self, sz):
    buff = TSocket.TSocket.read(self, sz)
    self._IncrementVarz(self._varz_bytes_recv, len(buff))
    return buff

  @wrap_health_exceptions()
  def write(self, buff):
    TSocket.TSocket.write(self, buff)
    self._IncrementVarz(self._varz_bytes_sent, len(buff))

  @wrap_health_exceptions()
  def open(self):
    TSocket.TSocket.open(self)
    self._IncrementVarz(self._varz_connections, 1)

  # Specifically not wrapped to prevent re-entrency issues
  def close(self):
    self._IncrementVarz(self._varz_connections, -1)
    TSocket.TSocket.close(self)

  def testConnection(self):
    from gevent.select import select as gselect
    import select
    try:
      reads, _, _ = gselect([self.handle], [], [], 0)
      return True
    except select.error:
      return False
