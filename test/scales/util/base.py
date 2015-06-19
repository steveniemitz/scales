import unittest

from scales.constants import SinkProperties
from scales.message import Message
from test.scales.util.mocks import (MockSinkProvider, MockSink, MockSinkStack)

class SinkTestCase(unittest.TestCase):
  REQ_MSG_SENTINEL = Message()
  MSG_SENTINEL = object()
  STREAM_SENTINEL = object()
  SINK_CLS = None
  _open_delay = 0

  def _prepareSinkStack(self):
    self.sink_stack = MockSinkStack()

    def save_message(sink_stack, context, stream, msg):
      self.return_message = msg
      self.return_stream = stream

    terminator_sink = MockSink({ SinkProperties.Endpoint: None })
    terminator_sink.ProcessResponse = save_message
    self.sink_stack.Push(terminator_sink)

  def _submitTestMessage(self):
    self.sink.AsyncProcessRequest(self.sink_stack, self.REQ_MSG_SENTINEL, None, None)

  def _completeTestMessage(self):
    self.sink_stack.AsyncProcessResponse(self.STREAM_SENTINEL, self.MSG_SENTINEL)

  def customize(self, **kwargs):
    pass

  def _createSink(self):
    return self.SINK_CLS(self.mock_provider, self.sink_properties, self.global_properties)

  def setUp(self, **kwargs):
    self.return_message = None
    self.return_stream = None
    self.sink_properties = None
    self.global_properties = {
      SinkProperties.Label: 'mock'
    }
    self.customize(**kwargs)
    self.mock_provider = MockSinkProvider()
    self.sink = self._createSink()
    self.sink.Open().wait()
    self._prepareSinkStack()
