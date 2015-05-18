import collections

class VarzReceiver(object):
  VARZ_DATA = collections.defaultdict(int)

  @staticmethod
  def IncrementVarz(source, metric, amount=1):
    VarzReceiver.VARZ_DATA[(metric, source)] += amount

  @staticmethod
  def SetVarz(source, metric, value):
    VarzReceiver.VARZ_DATA[(metric, source)] = value