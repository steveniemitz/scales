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

BUFFER_SIZE = 1024 * 100 # 100kb
def pipe_io(src, dst):
  while True:
    read = src.read(BUFFER_SIZE)
    if read:
      dst.write(read)
    else:
      break

class Message(Marshallable):
  def __init__(self, msg_type):
    self._tag = None
    self._type = msg_type

  @staticmethod
  def _EnsureTag(tag):
    if tag is None:
      raise Exception("Tag was unset on message during serialization.")

  @property
  def tag(self):
    return self._tag

  @tag.setter
  def tag(self, value):
    self._tag = value

  def _EncodeTag(self, tag=None):
    if tag is None:
      tag = self.tag
    self._EnsureTag(tag)
    return [tag >> 16 & 0xff, tag >> 8 & 0xff, tag & 0xff] # Tag

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
    buf.write(pack('!h', len(self._ctx)))
    for k, v in self._ctx.iteritems():
      if not isinstance(k, basestring):
        raise NotImplementedError("Unsupported key type in context")
      k_len = len(k)
      buf.write(pack('!h%ds' % k_len, k_len, k))
      buf.write(pack('!h', v.GetSize()))
      v.Marshal(buf)

  def Marshal(self, buf):
    data_length = self._GetContextSize() + 2 + 2 + self._data.tell()
    self._WriteHeader(buf, data_length)
    self._WriteContext(buf)
    buf.write(pack('!hh', 0, 0)) # len(dst), len(dtab), both unsupported
    self._data.seek(0)
    pipe_io(self._data, buf)


class PingMessage(Message):
  def __init__(self):
    super(PingMessage, self).__init__(MessageType.Tping)

  def Marshal(self, buf):
    self._WriteHeader(buf, 0)


class DiscardedMessage(Message):
  def __init__(self, which, reason):
    super(DiscardedMessage, self).__init__(MessageType.BAD_Tdiscarded)
    self._reason = reason
    self._which = which

  def Marshal(self, buf):
    self._WriteHeader(buf, 3 + len(self._reason))
    buf.write(pack('!BBB', *self._EncodeTag(self._which)))
    buf.write(self._reason)


class RMessage(object):
  def __init__(self, msg_type, err=None):
    self._type = msg_type
    self._err = err

  @property
  def err(self):
    return self._err

  @property
  def type(self):
    return self._type

class RdispatchMessage(RMessage):
  class Rstatus:
    OK = 0
    ERROR = 1
    NACK = 2

  def __init__(self, buf=None, err=None):
    super(RdispatchMessage, self).__init__(MessageType.Rdispatch, err)
    self.buf = buf

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
    elif status == cls.Rstatus.NACK:
      return cls(err='The server returned a NACK')
    else:
      return cls(err=buf.read())
