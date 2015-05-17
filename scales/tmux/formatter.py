from struct import (pack, unpack)

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport.TTransport import TMemoryBuffer
from thrift.Thrift import TMessageType

from scales.message import (
  RdispatchMessage,
  RerrorMessage
)
from scales.ttypes import MessageType

class Tag(object):
  KEY = "__Tag"

  def __init__(self, tag):
    self._tag = tag

  def Encode(self):
    return [self._tag >> 16 & 0xff,
            self._tag >>  8 & 0xff,
            self._tag       & 0xff]


class MessageSerializer(object):
  def __init__(self):
    self._marshal_map = {
      MessageType.Tdispatch: self._Marshal_Tdispatch,
      MessageType.Tdiscarded: self._Marshal_Tdiscarded,
      MessageType.BAD_Tdiscarded: self._Marshal_Tdiscarded,
    }
    self._unmarshal_map = {
      MessageType.Rdispatch: self._Unmarshal_Rdispatch,
      MessageType.Rerr: self._Unmarshal_Rerror,
      MessageType.BAD_Rerr: self._Unmarshal_Rerror,
    }

  @staticmethod
  def _Marshal_Tdispatch(msg, buf):
    MessageSerializer._WriteContext(msg._ctx, buf)
    buf.write(pack('!hh', 0, 0)) # len(dst), len(dtab), both unsupported
    tbuf = TMemoryBuffer()
    tbuf._buffer = buf
    prot = TBinaryProtocolAccelerated(tbuf, True, True)
    cli = msg._service(prot)
    getattr(cli, 'send_' + msg._method)(*msg._args)
    is_one_way = not hasattr(cli, 'recv_' + msg._method)

    return is_one_way, (msg._service, msg._method)

  @staticmethod
  def _Marshal_Tdiscarded(msg, buf):
    buf.write(pack('!BBB', *Tag(msg._which).Encode()))
    buf.write(msg._reason)
    return True, None

  @staticmethod
  def _WriteContext(ctx, buf):
    buf.write(pack('!h', len(ctx)))
    for k, v in ctx.iteritems():
      if not isinstance(k, basestring):
        raise NotImplementedError("Unsupported key type in context")
      k_len = len(k)
      buf.write(pack('!h%ds' % k_len, k_len, k))
      buf.write(pack('!h', len(v)))
      v.Marshal(buf)

  @staticmethod
  def _ReadContext(buf):
    for _ in range(2):
      sz, = unpack('!h', buf.read(2))
      buf.read(sz)

  @staticmethod
  def _Unmarshal_Rdispatch(buf, ctx):
    status, nctx = unpack('!bh', buf.read(3))
    for n in range(0, nctx):
      MessageSerializer._ReadContext(buf)

    if status == RdispatchMessage.Rstatus.OK:
      tbuf = TMemoryBuffer()
      tbuf._buffer = buf
      prot = TBinaryProtocolAccelerated(tbuf)
      client_cls, method = ctx
      cli = client_cls(prot)
      response = getattr(cli, 'recv_%s' % method)()
      buf.close()

      return RdispatchMessage(response)
    elif status == RdispatchMessage.Rstatus.NACK:
      return RdispatchMessage(err='The server returned a NACK')
    else:
      return RdispatchMessage(err=buf.read())

  @staticmethod
  def _Unmarshal_Rerror(buf, ctx):
    why = buf.read()
    return RerrorMessage(why)

  def Unmarshal(self, tag, msg_type, data, ctx):
    msg_type_cls = self._unmarshal_map[msg_type]
    if callable(msg_type_cls) or isinstance(msg_type_cls, staticmethod):
      msg = msg_type_cls(data, ctx)
    else:
      msg = msg_type_cls.Unmarshal(data, ctx)
    msg.tag = tag
    return msg

  def Marshal(self, msg, buf):
    marshaller = self._marshal_map[msg.type]
    return marshaller(msg, buf)


