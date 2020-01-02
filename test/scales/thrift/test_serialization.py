import codecs
import unittest

from scales.compat import BytesIO
from scales.message import MethodCallMessage
from scales.thrift.serializer import MessageSerializer
from test.scales.thrift.gen_py.hello import Hello

class ThriftSerializerTestCase(unittest.TestCase):
  def testSerialization(self):
    expected = codecs.decode(b'gAEAAQAAAAJoaQAAAAALAAEAAAARdGhpc19pc190ZXN0X2RhdGEA', 'base64')
    s = MessageSerializer(Hello.Iface)
    mcm = MethodCallMessage(Hello.Iface, 'hi', ('this_is_test_data',), {})
    buf = BytesIO()
    s.SerializeThriftCall(mcm, buf)
    self.assertEqual(buf.getvalue(), expected)

  def testDeserialization(self):
    raw_message = codecs.decode(b'gAEAAgAAAAJoaQAAAAALAAAAAAAYdGhpcyBpcyBhIHJldHVybiBtZXNzYWdlAA==', 'base64')
    s = MessageSerializer(Hello.Iface)
    buf = BytesIO(raw_message)
    ret = s.DeserializeThriftCall(buf)
    self.assertEqual(ret.return_value, 'this is a return message')

if __name__ == '__main__':
  unittest.main()
