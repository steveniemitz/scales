import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport.TTransport import TMemoryBuffer
from thrift.Thrift import (
  TApplicationException,
  TMessageType
)

from ..message import MethodReturnMessage

class MessageSerializer(object):
  """A serializer that can serialize and deserialize thrift method calls.

  This relies on the generated thrift args and return value classes created
  by the thrift compiler.
  """

  @staticmethod
  def SerializeThriftCall(msg, buf):
    """Serialize a MethodCallMessage to a stream

    Args:
      msg - The MethodCallMessage to serialize.
      buf - The buffer to serialize into.
    """
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
  def DeserializeThriftCall(buf, ctx):
    """Deserialize a stream and context to a MethodReturnMessage.

    Args:
      buf - The buffer.
      ctx - The context from serialization.

    Returns:
      A MethodCallMessage.
    """

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
      return MethodReturnMessage(error=x)

    result_cls = getattr(module, '%s_result' % method, None)
    if result_cls:
      result = result_cls()
      result.read(iprot)
    else:
      result = None
    iprot.readMessageEnd()

    if not result:
      return MethodReturnMessage()
    if getattr(result, 'success', None) is not None:
      return MethodReturnMessage(return_value=result.success)

    result_spec = getattr(result_cls, 'thrift_spec', None)
    if result_spec:
      exceptions = result_spec[1:]
      for e in exceptions:
        attr_val = getattr(result, e[2], None)
        if attr_val is not None:
          return MethodReturnMessage(error=attr_val)

    return MethodReturnMessage(TApplicationException(
      TApplicationException.MISSING_RESULT, "%s failed: unknown result" % method))

