from __future__ import absolute_import

import functools

from thrift.transport import TTransport

from .varz import VarzReceiver

class VarzSocketWrapper(TTransport.TTransportBase):
  """A wrapper for Thrift sockets that records various varz about the socket."""
  class Varz(object):
    def __init__(self, service_tag, transport_tag):
      def inc_all(metric, amount):
        VarzReceiver.IncrementVarz(service_tag, metric, amount)
        VarzReceiver.IncrementVarz(transport_tag, metric, amount)
      base_tag = 'scales.socket.%s'
      self.bytes_recv = functools.partial(
          inc_all, base_tag % 'bytes_recv')
      self.bytes_sent = functools.partial(
          inc_all, base_tag % 'bytes_sent')
      self.num_connections = functools.partial(
          inc_all, base_tag % 'num_connections')
      self.tests_failed = functools.partial(
          inc_all, base_tag % 'tests_failed', 1)
      self.connects = functools.partial(
          inc_all, base_tag % 'connects', 1)

  def __init__(self, socket, varz_tag, test_connections=False):
    self._socket = socket
    self._test_connections = test_connections
    self._varz = self.Varz(varz_tag, '%s.%d' % (self.host, self.port))

  @property
  def host(self):
    return self._socket.host

  @property
  def port(self):
    return self._socket.port

  def isOpen(self):
    return self._socket.isOpen()

  def read(self, sz):
    buff = self._socket.read(sz)
    self._varz.bytes_recv(len(buff))
    return buff

  def flush(self):
    pass

  def write(self, buff):
    self._socket.write(buff)
    self._varz.bytes_sent(len(buff))

  def open(self):
    self._socket.open()
    self._varz.connects()
    self._varz.num_connections(1)

  def close(self):
    self._varz.num_connections(-1)
    self._socket.close()

  def testConnection(self):
    if not self._test_connections:
      return True

    from gevent.select import select as gselect
    import select
    try:
      reads, _, _ = gselect([self._socket.handle], [], [], 0)
      return True
    except select.error:
      self._varz.tests_failed()
      return False
