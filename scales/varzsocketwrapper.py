from __future__ import absolute_import

from thrift.transport import TTransport

from scales.varz import VarzReceiver

_VARZ_PREFIX = 'scales.socket'

class VarzSocketWrapper(TTransport.TTransportBase):
  def __init__(self, socket, varz_tag):
    self._socket = socket
    self._InitVarz(varz_tag)

  def _InitVarz(self, varz_tag):
    make_tag = lambda metric: '.'.join([_VARZ_PREFIX, metric])
    self._varz_bytes_recv = make_tag('bytes_recv')
    self._varz_bytes_sent = make_tag('bytes_sent')
    self._varz_connections = make_tag('num_connections')
    self._service_source = varz_tag
    self._transport_source = '%s.%d' % (self.host, self.port)

  @property
  def host(self):
    return self._socket.host

  @property
  def port(self):
    return self._socket.port

  def _IncrementVarz(self, metric, amount):
    VarzReceiver.IncrementVarz(metric, self._service_source, amount)
    VarzReceiver.IncrementVarz(metric, self._transport_source, amount)

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
