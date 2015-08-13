from struct import pack
import unittest
from cStringIO import StringIO

import gevent

from scales.thrift.sink import SocketTransportSink, ChannelConcurrencyError
from scales.constants import ChannelState
from test.scales.util.mocks import MockSocket
from test.scales.util.base import SinkTestCase

class ExpectedException(Exception): pass

class ThriftSinkTestCase(SinkTestCase):
  SINK_CLS = SocketTransportSink

  def _createSink(self):
    self.mock_socket = MockSocket('localhost', 8080)
    return SocketTransportSink(self.mock_socket, 'test')

  def _processTransaction(self, request, response, open=None, close=None, read=None, write=None, sink=None, do_yield=True):
    write_data = []
    read_data = [pack('!i', len(response)), response]
    def write_cb(buff):
      write_data.append(buff)
      if write:
        write(buff)

    def read_cb(sz):
      if read:
        read(sz)
      return read_data.pop(0)

    if not sink:
      socket = MockSocket('localhost', 8080, open, close, read_cb, write_cb)
      sink = SocketTransportSink(socket, 'test')
      self.sink = sink
      sink.Open().get()

    stream = StringIO(request)
    self._prepareSinkStack()
    sink_stack = self.sink_stack
    sink.AsyncProcessRequest(sink_stack, self.REQ_MSG_SENTINEL, stream, {})
    # Yield
    if do_yield:
      gevent.sleep()
      gevent.sleep()
    return read_data, write_data, sink, sink_stack

  def testBasicTransaction(self):
    request = 'test_request'
    response = 'test_response'
    read_data, write_data, _, _ = self._processTransaction(request, response)

    # The sink should write frame_len + 'test_request' and read 'response'.
    self.assertEqual(write_data, [pack('!i', len(request)) + request])
    self.assertEqual(read_data, [])
    self.assertEqual(self.return_stream.getvalue(), response)

  def testSocketOpenRaisesException(self):
    def open_cb():
      raise ExpectedException()
    self.assertRaises(ExpectedException, lambda: self._processTransaction('', '', open_cb))
    self.assertEqual(self.sink.state, ChannelState.Closed)

  def testConcurrency(self):
    request = 'test_request'
    response = 'test_response'

    _, _, sink, sink_stack = self._processTransaction(request, response, do_yield=False)
    _, _, _, sink_stack_2 = self._processTransaction(request, response, sink=sink, do_yield=False)
    self.assertIsNotNone(sink_stack_2.return_message)
    # The second request should fail immediately w/ a concurrency violation
    self.assertIsInstance(sink_stack_2.return_message.error, ChannelConcurrencyError)
    gevent.sleep(0)
    gevent.sleep(0)
    # The first request should succeed.
    self.assertEqual(sink_stack.return_stream.getvalue(), response)

  def testWriteFailure(self):
    def write_cb(buff):
      raise ExpectedException()
    self._processTransaction('test', 'unexpected', write=write_cb)
    self.assertEqual(self.sink.state, ChannelState.Closed)
    self.assertIsInstance(self.sink_stack.return_message.error, ExpectedException)

  def testReadFailure(self):
    def read_cb(sz):
      raise ExpectedException()
    self._processTransaction('test', 'unexpected', read=read_cb)
    self.assertEqual(self.sink.state, ChannelState.Closed)
    self.assertIsInstance(self.sink_stack.return_message.error, ExpectedException)

if __name__ == '__main__':
  unittest.main()
