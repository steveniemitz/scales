from struct import (pack, unpack)
from types import (MessageType)

class Marshallable(object):
  def Marshal(self, buf):
    raise NotImplementedError()

  @classmethod
  def Unmarshal(cls, buf):
    raise NotImplementedError()

  def GetSize(self):
    raise NotImplementedError()


class Deadline(Marshallable):
  def __init__(self, timeout):
    """
    Args:
      timeout - The timeout in seconds
    """
    import  time
    self._ts = long(time.time()) * 1000000000 # Nanoseconds
    self._timeout = long(timeout * 1000000000)

  def Marshal(self, buf):
    buf.write(pack('!qq', self._ts, self._timeout))

  def GetSize(self):
    return 16


class Message(Marshallable):
  def __init__(self, msg_type):
    self._tag = None
    self._type = msg_type

  def _EnsureTag(self):
    if not self.tag:
      raise Exception("Tag was unset on message during serialization.")

  @property
  def tag(self):
    return self._tag

  @tag.setter
  def tag(self, value):
    self._tag = value

  def _EncodeTag(self):
    self._EnsureTag()
    return [self._tag >> 16 & 0xff, self._tag >> 8 & 0xff, self._tag & 0xff] # Tag

  def _WriteHeader(self, buf, data_len):
    total_len = 1 + 3 + data_len
    buf.write(pack('!ibBBB',
                   total_len,
                   self._type,
                   *self._EncodeTag()))

  def Marshal(self, buf):
    raise NotImplementedError()

  @classmethod
  def Unmarshal(cls, buf):
    raise NotImplementedError()

  def GetSize(self):
    raise None

class DispatchMessage(Message):
  def __init__(self, data, ctx=None, dst=None, dtab=None):
    super(DispatchMessage, self).__init__(MessageType.Tdispatch)
    self._tag = None
    self._ctx = ctx or {}
    self._dst = dst
    self._dtab = dtab
    self._data = data

  def _GetContextSize(self):
    n = 2
    if not any(self._ctx):
      return n
    for k, v in self._ctx.iteritems():
      n += 2 + len(k)
      n += 2 + v.GetSize()
    return n

  def _WriteContext(self, buf):
    if not any(self._ctx):
      buf.write(pack('!h', 0))

    buf.write(pack('!h', len(self._ctx)))
    for k, v in self._ctx.iteritems():
      if not isinstance(k, basestring):
        raise NotImplementedError("Unsupported key type in context")
      k_len = len(k)
      buf.write(pack('!h%ds' % k_len, k_len, k))
      buf.write(pack('!h', v.GetSize()))
      v.Marshal(buf)

  def Marshal(self, buf):
    data_length = self._GetContextSize() + 2 + 2 + len(self._data)
    self._WriteHeader(buf, data_length)
    self._WriteContext(buf)
    buf.write(pack('!hh', 0, 0)) # len(dst), len(dtab), both unsupported
    buf.write(self._data)


class PingMessage(Message):
  def __init__(self):
    super(PingMessage, self).__init__(MessageType.Tping)

  def Marshal(self, buf):
    self._WriteHeader(buf, 0)


class RdispatchMessage(Message):
  class Rstatus:
    OK = 0
    ERROR = 1
    NACK = 2

  def __init__(self, buf=None, err=None):
    super(RdispatchMessage, self).__init__(MessageType.Rdispatch)
    self.buf = buf
    self.err = err

  @staticmethod
  def _ReadContext(buf):
    for _ in range(2):
      sz, = unpack('!h', buf.read(2))
      buf.read(sz)

  @classmethod
  def Unmarshal(cls, buf):
    status, nctx = unpack('!bh', buf.read(3))
    for n in range(0, nctx):
      cls._ReadContext(buf)

    if status == cls.Rstatus.OK:
      return cls(buf)
    else:
      return cls(err=buf.read())
