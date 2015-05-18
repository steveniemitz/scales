from ..constants import Enum

class MessageType(Enum):
  Tdispatch = 2
  Rdispatch = -2
  Rerr = -128
  BAD_Rerr = 127

  Tping = 65
  Rping = -65

  Tdiscarded = 66
  BAD_Tdiscarded = -62


class Rstatus(Enum):
  OK = 0
  ERROR = 1
  NACK = 2


class Headers(object):
  MessageType = '__MessageType'
