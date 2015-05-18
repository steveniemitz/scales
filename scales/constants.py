class Enum(object): pass

class DispatcherState(Enum):
  UNINITIALIZED = 0
  STARTING = 1
  RUNNING = 2
  SHUTTING_DOWN = 3
  STOPPED = 4
  FAULTED = 5
  PING_TIMEOUT = 6

  DRAINING = 10
