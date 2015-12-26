from struct import (pack, unpack)

from ..constants import TransportHeaders
from ..message import (
  MethodCallMessage,
  MethodDiscardMessage,
  MethodReturnMessage,
  ServerError,
  Deadline
)
from ..mux.sink import Tag
from ..thrift.serializer import MessageSerializer as ThriftMessageSerializer
from .protocol import (
  Rstatus,
  MessageType
)


class MessageSerializer(object):
  """A serializer that can serialize/deserialize method calls into the ThriftMux
  wire format."""
  def __init__(self, service_cls):
    self._marshal_map = {
      MethodReturnMessage: self._Marshal_Rdispatch,

      MethodCallMessage: self._Marshal_Tdispatch,
      MethodDiscardMessage: self._Marshal_Tdiscarded,
    }
    self._unmarshal_map = {
      MessageType.Tdispatch: self._Unmarshal_Tdispatch,

      MessageType.Rdispatch: self._Unmarshal_Rdispatch,
      MessageType.Rerr: self._Unmarshal_Rerror,
      MessageType.BAD_Rerr: self._Unmarshal_Rerror,
    }
    if service_cls:
      self._thrift_serializer = ThriftMessageSerializer(service_cls)

  def _Marshal_Tdispatch(self, msg, buf, headers):
    headers[TransportHeaders.MessageType] = MessageType.Tdispatch
    self._WriteContext(msg.public_properties, buf)
    buf.write(pack('!hh', 0, 0)) # len(dst), len(dtab), both unsupported
    self._thrift_serializer.SerializeThriftCall(msg, buf)

  @staticmethod
  def _Marshal_Tdiscarded(msg, buf, headers):
    headers[TransportHeaders.MessageType] = MessageType.Tdiscarded
    buf.write(pack('!BBB', *Tag(msg.which).Encode()))
    buf.write(msg.reason)

  def _Marshal_Rdispatch(self, msg, buf, headers):
    headers[TransportHeaders.MessageType] = MessageType.Rdispatch
    buf.write(pack('!b', Rstatus.OK))
    MessageSerializer._WriteContext(msg.public_properties, buf)
    self._thrift_serializer.SerializeThriftResponse(msg, buf)

  @staticmethod
  def _WriteContext(ctx, buf):
    buf.write(pack('!h', len(ctx)))
    for k, v in ctx.iteritems():
      if not isinstance(k, basestring):
        raise NotImplementedError("Unsupported key type in context")
      k_len = len(k)
      buf.write(pack('!h%ds' % k_len, k_len, k))
      if isinstance(v, Deadline):
        buf.write(pack('!h', 16))
        buf.write(pack('!qq', v._ts, v._timeout))
      else:
        raise NotImplementedError("Unsupported value type in context.")

  @staticmethod
  def _ReadContextTuple(buf):
    sz, = unpack('!h', buf.read(2))
    key = buf.read(sz)
    sz, = unpack('!h', buf.read(2))
    value = buf.read(sz)

    if key == "com.twitter.finagle.Deadline":
      ts, timeout = unpack('!qq', value)
      value = Deadline(timeout, ts)
    return key, value

  @staticmethod
  def _ReadContext(buf):
    nctx, = unpack('!h', buf.read(2))
    ret = {}
    for n in range(nctx):
      k, v = MessageSerializer._ReadContextTuple(buf)
      ret[k] = v
    return ret

  def _Unmarshal_Rdispatch(self, buf):
    status, nctx = unpack('!bh', buf.read(3))
    for n in range(0, nctx):
      self._ReadContextTuple(buf)

    if status == Rstatus.OK:
      return self._thrift_serializer.DeserializeThriftReturnMessage(buf)
    elif status == Rstatus.NACK:
      return MethodReturnMessage(error=ServerError('The server returned a NACK'))
    else:
      return MethodReturnMessage(error=ServerError(buf.read()))

  @staticmethod
  def _Unmarshal_Rerror(buf):
    why = buf.read()
    return MethodReturnMessage(error=ServerError(why))

  def _Unmarshal_Tdispatch(self, buf):
    ctx = self._ReadContext(buf)
    dst = self._ReadContext(buf)
    dtab = self._ReadContext(buf)
    return self._thrift_serializer.DeserializeThriftCallMessage(buf)

  def Unmarshal(self, tag, msg_type, buf):
    """Deserialize a message from a stream.

    Args:
      tag - The tag of the message.
      msg_type - The message type intended to be deserialized.
      buf - The stream to deserialize from.
    Returns:
      A Message.
    """
    unmarshaller = self._unmarshal_map[msg_type]
    return unmarshaller(buf)

  def Marshal(self, msg, buf, headers):
    """Serialize a message into a stream.

    Args:
      msg - The message to serialize.
      buf - The stream to serialize into.
      headers - (out) Optional headers associated with the message.
    """
    marshaller = self._marshal_map[msg.__class__]
    marshaller(msg, buf, headers)

  @staticmethod
  def EncodeTag(tag):
    return [tag >> 16 & 0xff, tag >> 8 & 0xff, tag & 0xff] # Tag

  @staticmethod
  def BuildHeader(tag, msg_type, data_len):
    total_len = 1 + 3 + data_len
    return pack('!ibBBB',
        total_len,
        msg_type,
        *MessageSerializer.EncodeTag(tag))