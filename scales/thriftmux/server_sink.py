import logging
from cStringIO import StringIO

from ..constants import ConnectionRole, SinkProperties, MessageProperties, TransportHeaders
from ..message import MethodReturnMessage
from ..mux.sink import MuxSocketTransportSink
from ..sink import SinkProvider, MessageSinkStack, SocketTransportSinkProvider
from .sink import ThriftMuxMessageSerializerSink
from .protocol import MessageType
from .serializer import MessageSerializer

ROOT_LOG = logging.getLogger('scales.thriftmux')

class ThriftMuxServerSocketTransportSink(MuxSocketTransportSink):
  def __init__(self, socket, service, next_provider, sink_properties, global_properties):
    super(ThriftMuxServerSocketTransportSink, self).__init__(socket, service, next_provider, sink_properties, global_properties, ConnectionRole.Server)
    self._log = ROOT_LOG.getChild('ServerTransportSink')
    self._server_tag_map = {}
    self.next_sink = next_provider.CreateSink(global_properties)

  def _CheckInitialConnection(self):
    pass

  def _OnTimeout(self, tag):
    pass

  def _BuildHeader(self, tag, msg_type, data_len):
    pass

  def _ProcessRecv(self, stream):
    msg_type, tag = ThriftMuxMessageSerializerSink.ReadHeader(stream)
    if msg_type == MessageType.Tping:
      self._Send(MessageSerializer.BuildHeader(tag, MessageType.Rping, 0), {})
    elif msg_type in (MessageType.Tdiscarded, MessageType.BAD_Tdiscarded):
      self._DiscardMessage(tag)
    elif msg_type == MessageType.Tdispatch:
      self._DispatchMessage(stream, tag)
    else:
      self._log.error("Invalid message type recieved: %s", msg_type)

  def _DispatchMessage(self, stream, tag):
    stream.seek(0)
    stack = MessageSinkStack()
    stack.Push(self, tag)
    self._server_tag_map[tag] = stack
    self.next_sink.AsyncProcessRequest(stack, None, stream, {})

  def _DiscardMessage(self, tag):
    self._log.debug("Ignoring request to discard tag %s", tag)

  def _DiscardTag(self, tag):
    self._server_tag_map.pop(tag, None)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    tag = context
    body = stream.getvalue()
    header = MessageSerializer.BuildHeader(tag, MessageType.Rdispatch, len(body))
    self._Send(header + body, {})
    self._DiscardTag(tag)

ThriftMuxServerSocketTransportSink.Builder = SocketTransportSinkProvider(ThriftMuxServerSocketTransportSink)

class ThriftMuxServerMessageSerializerSink(ThriftMuxMessageSerializerSink):
  def AsyncProcessRequest(self, sink_stack, msg, stream, headers):
    msg = self._DeserializeStream(stream, headers)
    if isinstance(msg, MethodReturnMessage):
      sink_stack.AsyncProcessResponseMessage(msg)
      return

    deadline = headers.get(Deadline.KEY)

    sink_stack.Push(self, headers)
    self.next_sink.AsyncProcessRequest(sink_stack, msg, stream, headers)

  def AsyncProcessResponse(self, sink_stack, context, stream, msg):
    buf = StringIO()
    headers = {}
    recv_headers = context

    headers[TransportHeaders.ThriftMethod] = recv_headers[TransportHeaders.ThriftMethod]
    headers[TransportHeaders.ThriftSequenceId] = recv_headers[TransportHeaders.ThriftSequenceId]
    self._serializer.Marshal(msg, buf, headers)
    sink_stack.AsyncProcessResponseStream(buf)

ThriftMuxServerMessageSerializerSink.Builder = SinkProvider(ThriftMuxServerMessageSerializerSink)