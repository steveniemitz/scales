from struct import (
  calcsize,
  pack,
  unpack,
  Struct
)
import itertools

class Structs(object):
  Byte  = Struct('!B')
  Int16 = Struct('!h')
  Int32 = Struct('!i')
  Int64 = Struct('!q')

class BinaryReader(object):
  def __init__(self, buf):
    self._buf = buf

  def ReadString(self):
    str_len, = Structs.Int16.unpack(self._buf.read(2))
    str_data = self._buf.read(str_len)
    return str_data

  def ReadInt16(self):
    data, = Structs.Int16.unpack(self._buf.read(2))
    return data

  def ReadInt32(self):
    data, = Structs.Int32.unpack(self._buf.read(4))
    return data

  def ReadInt64(self):
    data, = Structs.Int64.unpack(self._buf.read(8))
    return data

  def ReadInt32Array(self):
    num_to_read = self.ReadInt32()
    return unpack('!%di' % num_to_read, self._buf.read(4 * num_to_read))

  def Unpack(self, fmt):
    to_read = calcsize(fmt)
    return unpack(fmt, self._buf.read(to_read))

class BinaryWriter(object):
  def __init__(self, buf):
    self._buf = buf

  def WriteByte(self, val):
    self._buf.write(Structs.Byte.pack(val))

  def WriteInt16(self, val):
    self._buf.write(Structs.Int16.pack(val))

  def WriteInt32(self, val):
    self._buf.write(Structs.Int32.pack(val))

  def WriteInt64(self, val):
    self._buf.write(Structs.Int64.pack(val))

  def WriteString(self, val):
    self._buf.write(Structs.Int16.pack(len(val)))
    self._buf.write(val)

  def WriteBinary(self, val):
    self._buf.write(Structs.Int32.pack(len(val)))
    self._buf.write(val)

  def WriteRaw(self, val):
    self._buf.write(val)

  def WriteStruct(self, s, *vals):
    self._buf.write(s.pack(*vals))

  def Pack(self, fmt, *args):
    self._buf.write(pack(fmt, *args))
