from __future__ import absolute_import

from thrift.protocol.TJSONProtocol import TJSONProtocol, JTYPES, CTYPES
from thrift.Thrift import TType

try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO

try:
  import simplejson as json
except ImportError:
  import json

class TFastJSONProtocol(TJSONProtocol):
  class InitContext(object):
    """A context for initializing the reader"""
    def __init__(self, msg):
      self.msg = msg

    def read(self):
      return self.msg

    def write(self, obj):
      self.msg = obj

    def get_buffer(self):
      return self.msg

  class ArrayContext(object):
    """A context for reading from an array."""
    def __init__(self, array):
      assert isinstance(array, list)
      self.arr = array
      self.idx = 0

    def read(self):
      idx = self.idx
      self.idx += 1
      return self.arr[idx]

    def write(self, val):
      self.arr.append(val)

    def get_buffer(self):
      return self.arr

  class ObjectContext(object):
    """A context for reading from an object."""
    def __init__(self, obj):
      assert isinstance(obj, dict)
      self.obj = list(obj.items())
      self.field = None
      self.idx = 0

    def readFieldBegin(self):
      id = 0
      if self.idx >= len(self.obj):
        ttype = TType.STOP
      else:
        obj = self.obj[self.idx]
        id = int(obj[0])
        assert len(obj[1]) == 1
        self.field = obj[1].items()[0]
        ttype = JTYPES[self.field[0]]
      return (None, ttype, id)

    def readFieldEnd(self):
      self.idx += 1

    def writeFieldBegin(self, name, ttype, id):
      self.field = [
        str(id),
        [CTYPES[ttype], None]
      ]

    def writeFieldEnd(self):
      self.field[1] = dict([self.field[1]])
      self.obj.append(self.field)
      self.field = None

    def read(self):
      return self.field[1]

    def write(self, obj):
      self.field[1][1] = obj

    def get_buffer(self):
      return dict(self.obj)

  class MapContext(object):
    """A context for reading from a map."""
    def __init__(self, obj):
      self.map = list(obj.items())
      self.map_idx = 0
      self.idx = 0
      self._read_next()

    def _read_next(self):
      if self.map_idx < len(self.map):
        self.obj = self.map[self.map_idx]
        self.idx = 0
        self.map_idx += 1

    def read(self):
      idx, self.idx = self.idx, self.idx + 1
      o = self.obj[idx]
      if self.idx == 2:
        self._read_next()
      return o

    def write(self, value):
      if self.idx == 0:
        self.map.append([None, None])
      self.map[-1][self.idx] = value
      self.idx += 1
      if self.idx == 2:
        self.idx = 0

    def get_buffer(self):
      return dict(self.map)

  def __init__(self, trans):
    TJSONProtocol.__init__(self, trans)
    self._stack = []
    self._ctx = None

  def _StartReadContext(self, ctx_type):
    next_ctx = ctx_type(self._ctx.read())
    self._stack.append(self._ctx)
    self._ctx = next_ctx

  def _StartWriteContext(self, ctx_type, init):
    next_ctx = ctx_type(init)
    self._stack.append(self._ctx)
    self._ctx = next_ctx

  def readJSONArrayStart(self):
    self._StartReadContext(self.ArrayContext)

  def readJSONObjectStart(self):
    self._StartReadContext(self.ObjectContext)

  def _EndReadContext(self):
    self._ctx = self._stack.pop()

  readMessageEnd = _EndReadContext
  readJSONObjectEnd = _EndReadContext
  readJSONArrayEnd = _EndReadContext

  def _EndWriteContext(self):
    curr = self._ctx.get_buffer()
    self._ctx = self._stack.pop()
    self._ctx.write(curr)

  def _readTransport(self):
    js = StringIO()
    while True:
      data = self.trans.read(4096)
      if not data:
        break
      js.write(data)
    return js.getvalue()

  def readMessageBegin(self):
    if hasattr(self.trans, 'getvalue'):
      js = self.trans.getvalue()
    else:
      js = self._readTransport()

    message = json.loads(js)
    self._ctx = self.InitContext(message)
    self._stack = []
    return TJSONProtocol.readMessageBegin(self)

  def readJSONString(self, skipContext):
    return self._ctx.read()

  def readJSONInteger(self):
    return int(self._ctx.read())

  def readJSONDouble(self):
    return float(self._ctx.read())

  def readFieldBegin(self):
    return self._ctx.readFieldBegin()

  def readFieldEnd(self):
    return self._ctx.readFieldEnd()

  def readMapBegin(self):
    self.readJSONArrayStart()
    keyType = JTYPES[self.readJSONString(False)]
    valueType = JTYPES[self.readJSONString(False)]
    size = self.readJSONInteger()
    self._StartReadContext(self.MapContext)
    return (keyType, valueType, size)

  def writeMessageBegin(self, name, request_type, seqid):
    self._StartWriteContext(self.InitContext, None)
    TJSONProtocol.writeMessageBegin(self, name, request_type, seqid)

  def writeMessageEnd(self):
    TJSONProtocol.writeMessageEnd(self)
    # The thrift JSON parser is very sensitive, it can't handle spaces after
    # commas or colons <table flip emoji>
    json.dump(self._ctx.get_buffer(), self.trans, separators=(',',':'))

  def writeJSONArrayStart(self):
    self._StartWriteContext(self.ArrayContext, [])

  def writeJSONArrayEnd(self):
    self._EndWriteContext()

  def writeJSONObjectStart(self):
    self._StartWriteContext(self.ObjectContext, {})

  def writeJSONObjectEnd(self):
    self._EndWriteContext()

  def writeFieldBegin(self, name, ttype, id):
    self._ctx.writeFieldBegin(name, ttype, id)

  def writeFieldEnd(self):
    self._ctx.writeFieldEnd()

  def writeMapBegin(self, ktype, vtype, size):
    self.writeJSONArrayStart()
    self.writeJSONString(CTYPES[ktype])
    self.writeJSONString(CTYPES[vtype])
    self.writeJSONNumber(size)
    self._StartWriteContext(self.MapContext, {})

  def writeJSONString(self, number):
    self._ctx.write(number)

  writeJSONNumber = writeJSONString

class TFastJSONProtocolFactory(object):
  def getProtocol(self, trans):
    return TFastJSONProtocol(trans)
