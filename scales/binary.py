from struct import (
  calcsize,
  pack,
  unpack
)

class BinaryReader(object):
  def __init__(self, buf):
    self._buf = buf

  def ReadString(self):
    str_len, = unpack('!h', self._buf.read(2))
    str_data = self._buf.read(str_len)
    return str_data

  def ReadInt16(self):
    data, = unpack('!h', self._buf.read(2))
    return data

  def ReadInt32(self):
    data, = unpack('!i', self._buf.read(4))
    return data

  def ReadInt64(self):
    data, = unpack('!q', self._buf.read(8))
    return data

  def Unpack(self, fmt):
    to_read = calcsize(fmt)
    return unpack(fmt, self._buf.read(to_read))

class BinaryWriter(object):
  def __init__(self, buf):
    self._buf = buf

  def WriteByte(self, val):
    self._buf.write(pack('!B', val))

  def WriteInt16(self, val):
    self._buf.write(pack('!h', val))

  def WriteInt32(self, val):
    self._buf.write(pack('!i', val))

  def WriteInt64(self, val):
    self._buf.write(pack('!q', val))

  def WriteString(self, val):
    self._buf.write(pack('!h', len(val)))
    self._buf.write(val)

  def WriteBinary(self, val):
    self._buf.write(pack('!i', len(val)))
    self._buf.write(val)

  def Pack(self, fmt, *args):
    self._buf.write(pack(fmt, *args))
