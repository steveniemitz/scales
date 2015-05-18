class Enum(object):
  pass

class MessageType(Enum):
  Tdispatch = 2
  Rdispatch = -2
  Rerr = -128
  BAD_Rerr = 127

  Tping = 65
  Rping = -65

  Tdiscarded = 66
  BAD_Tdiscarded = -62


class DispatcherState(Enum):
  UNINITIALIZED = 0
  STARTING = 1
  RUNNING = 2
  SHUTTING_DOWN = 3
  STOPPED = 4
  FAULTED = 5
  PING_TIMEOUT = 6

  DRAINING = 10
