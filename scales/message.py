import traceback
from struct import (pack, unpack)

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport.TTransport import TMemoryBuffer

from ttypes import (Enum, MessageType)

class Marshallable(object):
  def Marshal(self, buf):
    raise NotImplementedError()

  @classmethod
  def Unmarshal(cls, buf, ctx):
    raise NotImplementedError()


class Tmessage(object): pass
class Rmessage(object): pass

class Timeout(object):
  KEY = "__Timeout"

class Tag(object):
  KEY = "__Tag"

  def __init__(self, tag):
    self._tag = tag

  def Encode(self):
    return [self._tag >> 16 & 0xff,
            self._tag >>  8 & 0xff,
            self._tag       & 0xff]


class Deadline(Marshallable):
  def __init__(self, timeout):
    """
    Args:
      timeout - The timeout in seconds
    """
    import  time
    self._ts = long(time.time()) * 1000000000 # Nanoseconds
    self._timeout = long(timeout * 1000000000)

  def __len__(self):
    return 16

  def Marshal(self, buf):
    buf.write(pack('!qq', self._ts, self._timeout))


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
    super(Message, self).__init__()
    self._props = {}
    self._type = msg_type

  @property
  def properties(self):
    return self._props

  @property
  def type(self):
    return self._type

  @property
  def isResponse(self):
    return self._type < 0

  def Marshal(self, buf):
    raise NotImplementedError()

  @classmethod
  def Unmarshal(cls, buf, ctx):
    raise NotImplementedError()


class TdispatchMessage(Message):
  def __init__(self, service, method, args, ctx=None, dst=None, dtab=None):
    super(TdispatchMessage, self).__init__(MessageType.Tdispatch)
    self._ctx = ctx or {}
    self._dst = dst
    self._dtab = dtab
    self._service = service
    self._method = method
    self._args = args

  def _GetContextSize(self):
    n = 2
    if not any(self._ctx):
      return n
    for k, v in self._ctx.iteritems():
      n += 2 + len(k)
      n += 2 + len(v)
    return n

  def _WriteContext(self, buf):
    buf.write(pack('!h', len(self._ctx)))
    for k, v in self._ctx.iteritems():
      if not isinstance(k, basestring):
        raise NotImplementedError("Unsupported key type in context")
      k_len = len(k)
      buf.write(pack('!h%ds' % k_len, k_len, k))
      buf.write(pack('!h', len(v)))
      v.Marshal(buf)

  def Marshal(self, buf):
    self._WriteContext(buf)
    buf.write(pack('!hh', 0, 0)) # len(dst), len(dtab), both unsupported
    tbuf = TMemoryBuffer()
    tbuf._buffer = buf
    prot = TBinaryProtocolAccelerated(tbuf)
    cli = self._service(prot)
    getattr(cli, 'send_' + self._method)(*self._args)
    return self._service, self._method


class TpingMessage(Message):
  def __init__(self):
    super(TpingMessage, self).__init__(MessageType.Tping)

  def Marshal(self, buf):
    pass


class TdiscardedMessage(Message):
  def __init__(self, which, reason):
    super(TdiscardedMessage, self).__init__(MessageType.BAD_Tdiscarded)
    self._reason = reason
    self._which = which

  def Marshal(self, buf):
    buf.write(pack('!BBB', *Tag(self._which).Encode()))
    buf.write(self._reason)


class RpingMessage(Message):
  def __init__(self):
    super(RpingMessage, self).__init__(MessageType.Rping)

  @classmethod
  def Unmarshal(cls, buf, ctx):
    return RpingMessage()


class RdispatchMessage(Message):
  class Rstatus(Enum):
    OK = 0
    ERROR = 1
    NACK = 2

  def __init__(self, response=None, err=None):
    super(RdispatchMessage, self).__init__(MessageType.Rdispatch)
    self._response = response
    self._err = err

  @staticmethod
  def _ReadContext(buf):
    for _ in range(2):
      sz, = unpack('!h', buf.read(2))
      buf.read(sz)

  @classmethod
  def Unmarshal(cls, buf, ctx):
    status, nctx = unpack('!bh', buf.read(3))
    for n in range(0, nctx):
      cls._ReadContext(buf)

    if status == cls.Rstatus.OK:
      tbuf = TMemoryBuffer()
      tbuf._buffer = buf
      prot = TBinaryProtocolAccelerated(tbuf)
      client_cls, method = ctx
      cli = client_cls(prot)
      response = getattr(cli, 'recv_%s' % method)()
      buf.close()

      return cls(response)
    elif status == cls.Rstatus.NACK:
      return cls(err='The server returned a NACK')
    else:
      return cls(err=buf.read())

  @property
  def response(self):
    return self._response

  @property
  def error(self):
    return self._err


class RerrorMessage(Message):
  def __init__(self, err):
    super(RerrorMessage, self).__init__(MessageType.Rerr)
    self._err = err

  @classmethod
  def Unmarshal(cls, buf, ctx):
    why = buf.read()
    return cls(why)

  @property
  def error(self):
    return self._err

class MessageSerializer(object):
  MESSAGE_MAP = {
    MessageType.Rdispatch: RdispatchMessage,
    MessageType.Rping: RpingMessage,
    MessageType.Rerr: RerrorMessage,
    MessageType.BAD_Rerr: RerrorMessage,

    MessageType.Tping: TpingMessage
  }

  @staticmethod
  def Unmarshal(tag, msg_type, data, ctx):
    msg_type_cls = MessageSerializer.MESSAGE_MAP[msg_type]
    msg = msg_type_cls.Unmarshal(data, ctx)
    msg.tag = tag
    return msg


class SystemMessage(object): pass
class TimeoutMessage(SystemMessage): pass
class SystemErrorMessage(SystemMessage):
  def __init__(self, excr):
    self._exception = excr
    self._stack = traceback.format_exc()

  @property
  def exception(self):
    return self._exception

  @property
  def stack(self):
    return self._stack

