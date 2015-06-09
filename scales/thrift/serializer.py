import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAcceleratedFactory
from thrift.transport.TTransport import TMemoryBuffer
from thrift.Thrift import (
  TApplicationException,
  TMessageType
)

from ..message import MethodReturnMessage

class MessageSerializer(object):
  """A serializer that can serialize and deserialize thrift method calls.

  This relies on the generated thrift args and return value classes created
  by the thrift compiler to do the serialization/deserialization.
  """
  def __init__(
      self,
      service_cls,
      protocol_factory=TBinaryProtocolAcceleratedFactory()):
    """
    Args:
      service_cls - The thrift generated interface class.
      protocol_factory - A class implementing getProtocol(...).  By default,
       TBinaryProtocolAcceleratedFactory is used.
    """
    self._protocol_factory = protocol_factory
    self._seq_id = 0
    self._service_module = sys.modules[service_cls.__module__]

  def SerializeThriftCall(self, msg, buf):
    """Serialize a MethodCallMessage to a stream

    Args:
      msg - The MethodCallMessage to serialize.
      buf - The buffer to serialize into.
    """
    thrift_buffer = TMemoryBuffer()
    thrift_buffer._buffer = buf
    protocol = self._protocol_factory.getProtocol(thrift_buffer)
    method, args, kwargs = msg.method, msg.args, msg.kwargs
    is_one_way = not hasattr(self._service_module, '%s_result' % method)
    args_cls = getattr(self._service_module, '%s_args' % method)

    protocol.writeMessageBegin(
        msg.method,
        TMessageType.ONEWAY if is_one_way else TMessageType.CALL,
        self._seq_id)
    thrift_args = args_cls(*args, **kwargs)
    thrift_args.write(protocol)
    protocol.writeMessageEnd()

  def DeserializeThriftCall(self, buf):
    """Deserialize a stream and context to a MethodReturnMessage.

    Args:
      buf - The buffer.
      ctx - The context from serialization.

    Returns:
      A MethodCallMessage.
    """

    thrift_buffer = TMemoryBuffer()
    thrift_buffer._buffer = buf
    protocol = self._protocol_factory.getProtocol(thrift_buffer)

    (fn_name, msg_type, seq_id) = protocol.readMessageBegin()
    if msg_type == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(protocol)
      protocol.readMessageEnd()
      return MethodReturnMessage(error=x)

    result_cls = getattr(self._service_module, '%s_result' % fn_name, None)
    if result_cls:
      result = result_cls()
      result.read(protocol)
    else:
      result = None
    protocol.readMessageEnd()

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
      TApplicationException.MISSING_RESULT, "%s failed: unknown result" % fn_name))

