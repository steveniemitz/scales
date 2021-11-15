from __future__ import absolute_import

from gevent.socket import socket as gsocket
import socket
from scales import key_material

class ScalesSocket(object):
  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.handle = None

  def isOpen(self):
    return self.handle is not None

  def _resolveAddr(self):
    return socket.getaddrinfo(
      self.host,
      self.port,
      socket.AF_UNSPEC,
      socket.SOCK_STREAM,
      0,
      socket.AI_PASSIVE | socket.AI_ADDRCONFIG)

  def _create_socket(self, af, kind):
    sock = gsocket(af, kind)
    return sock


  def open(self):
    resolved = self._resolveAddr()
    for res in resolved:
      self.handle = self._create_socket(res[0], res[1])
      try:
        self.handle.connect(res[4])
      except socket.error as e:
        if res is not resolved[-1]:
          continue
        else:
          raise e
      break

  def close(self):
    if self.handle:
      self.handle.close()
      self.handle = None

  def readAll(self, sz):
    buff = b''
    have = 0
    while have < sz:
      chunk = self.read(sz - have)
      have += len(chunk)
      buff += chunk

      if len(chunk) == 0:
        raise EOFError()

    return buff

  def read(self, sz):
    return self.handle.recv(sz)

  def write(self, buff):
    sent = 0
    have = len(buff)
    while sent < have:
      plus = self.handle.send(buff)
      if plus == 0:
        raise EOFError()
      sent += plus
      buff = buff[plus:]

  def upgrade_to_tls(self, service_identifier):
    from gevent.ssl import SSLContext, SSLSocket
    import ssl
    ctx = SSLContext()
    ctx.check_hostname = False

    chain, key, crt = key_material.resolve_from_service_identifier(service_identifier)

    ctx.load_verify_locations(chain)
    ctx.load_cert_chain(crt, key)

    self.handle = SSLSocket(
      sock=self.handle,
      server_side=False,
      cert_reqs=ssl.CERT_REQUIRED,
      suppress_ragged_eofs=False,
      _context=ctx
    )
