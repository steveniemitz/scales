import sys

from struct import (pack, unpack)

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport.TTransport import TMemoryBuffer
from thrift.Thrift import (
  TApplicationException,
  TMessageType
)

from scales.message import (
  MethodCallMessage,
  MethodDiscardMessage,
  MethodReturnMessage,
  ServerError,
  Deadline
)
from scales.thriftmux.protocol import (
  Headers,
  RdispatchMessage,
  RerrorMessage,
  MessageType
)

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
      MethodCallMessage: self._Marshal_Tdispatch,
      MethodDiscardMessage: self._Marshal_Tdiscarded,
    }
    self._unmarshal_map = {
      MessageType.Rdispatch: self._Unmarshal_Rdispatch,
      MessageType.Rerr: self._Unmarshal_Rerror,
      MessageType.BAD_Rerr: self._Unmarshal_Rerror,
    }

  @staticmethod
  def _SerializeThriftCall(msg, buf):
    tbuf = TMemoryBuffer()
    tbuf._buffer = buf
    prot = TBinaryProtocolAccelerated(tbuf, True, True)
    service, method, args, kwargs = msg.service, msg.method, msg.args, msg.kwargs
    service_module = sys.modules[service.__module__]
    is_one_way = not hasattr(service_module, '%s_result' % method)
    args_cls = getattr(service_module, '%s_args' % method)

    prot.writeMessageBegin(msg.method, TMessageType.ONEWAY if is_one_way else TMessageType.CALL, 0)
    thrift_args = args_cls(*args, **kwargs)
    thrift_args.write(prot)
    prot.writeMessageEnd()

  @staticmethod
  def _DeserializeThriftCall(buf, ctx):
    tbuf = TMemoryBuffer()
    tbuf._buffer = buf
    iprot = TBinaryProtocolAccelerated(tbuf)

    client_cls, method = ctx
    module = sys.modules[client_cls.__module__]

    (fname, mtype, rseqid) = iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(iprot)
      iprot.readMessageEnd()
      return x

    result_cls = getattr(module, '%s_result' % method, None)
    if result_cls:
      result = result_cls()
      result.read(iprot)
    else:
      result = None
    iprot.readMessageEnd()

    if not result:
      return None
    if getattr(result, 'success', None) is not None:
      return result.success

    result_spec = getattr(result_cls, 'thrift_spec', None)
    if result_spec:
      exceptions = result_spec[1:]
      for e in exceptions:
        attr_val = getattr(result, e[2], None)
        if attr_val is not None:
          return attr_val

      return TApplicationException(TApplicationException.MISSING_RESULT, "%s failed: unknown result" % method)

  @staticmethod
  def _Marshal_Tdispatch(msg, buf, headers):
    headers[Headers.MessageType] = MessageType.Tdispatch
    MessageSerializer._WriteContext(msg.public_properties, buf)
    buf.write(pack('!hh', 0, 0)) # len(dst), len(dtab), both unsupported
    MessageSerializer._SerializeThriftCall(msg, buf)
    # It's odd, but even "oneway" thrift messages get a response
    # with finagle, so we need to allocate a tag and track them still.
    return msg.service, msg.method

  @staticmethod
  def _Marshal_Tdiscarded(msg, buf, headers):
    headers[Headers.MessageType] = MessageType.Tdiscarded
    buf.write(pack('!BBB', *Tag(msg.which.properties[Tag.KEY]).Encode()))
    buf.write(msg.reason)
    return None,

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
      response = MessageSerializer._DeserializeThriftCall(buf, ctx)
      buf.close()
      if isinstance(response, Exception):
        return MethodReturnMessage(error=response)
      else:
        return MethodReturnMessage(response)
    elif status == RdispatchMessage.Rstatus.NACK:
      return MethodReturnMessage(error=ServerError('The server returned a NACK'))
    else:
      return MethodReturnMessage(error=ServerError(buf.read()))

  @staticmethod
  def _Unmarshal_Rerror(buf, ctx):
    why = buf.read()
    return MethodReturnMessage(error=ServerError(why))

  def Unmarshal(self, tag, msg_type, data, ctx):
    msg_type_cls = self._unmarshal_map[msg_type]
    if callable(msg_type_cls) or isinstance(msg_type_cls, staticmethod):
      msg = msg_type_cls(data, ctx)
    else:
      msg = msg_type_cls.Unmarshal(data, ctx)
    msg.tag = tag
    return msg

  def Marshal(self, msg, buf, headers):
    marshaller = self._marshal_map[msg.__class__]
    return marshaller(msg, buf, headers)


