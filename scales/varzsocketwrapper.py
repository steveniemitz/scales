from __future__ import absolute_import

import collections

from thrift.transport import TTransport

VARZ_DATA = collections.defaultdict(int)
_VARZ_PREFIX = 'scales'

class VarzSocketWrapper(TTransport.TTransportBase):
  def __init__(self, socket, varz_tag):
    self._InitVarz(varz_tag)
    self._socket = socket

  def _InitVarz(self, varz_tag):
    make_tag = lambda metric: '_'.join([_VARZ_PREFIX, varz_tag, metric])
    self._varz_bytes_recv = make_tag('bytes_recv')
    self._varz_bytes_sent = make_tag('bytes_sent')
    self._varz_connections = make_tag('num_connections')

  @staticmethod
  def _IncrementVarz(metric, amount):
    VARZ_DATA[metric] += amount

  def isOpen(self):
    return self._socket.isOpen()

  def read(self, sz):
    buff = self._socket.read(sz)
    self._IncrementVarz(self._varz_bytes_recv, len(buff))
    return buff

  def flush(self):
    pass

  def write(self, buff):
    self._socket.write(buff)
    self._IncrementVarz(self._varz_bytes_sent, len(buff))

  def open(self):
    self._socket.open()
    self._IncrementVarz(self._varz_connections, 1)

  def close(self):
    self._IncrementVarz(self._varz_connections, -1)
    self._socket.close()

  def testConnection(self):
    from gevent.select import select as gselect
    import select
    try:
      reads, _, _ = gselect([self._socket.handle], [], [], 0)
      return True
    except select.error:
      return False
