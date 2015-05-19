"""A stub class for Varz collection."""

import collections

class VarzReceiver(object):
  """A stub class to receive varz from Scales."""
  VARZ_DATA = collections.defaultdict(int)

  @staticmethod
  def IncrementVarz(source, metric, amount=1):
    """Increment (source, metric) by amount"""
    VarzReceiver.VARZ_DATA[(metric, source)] += amount

  @staticmethod
  def SetVarz(source, metric, value):
    """Set (source, metric) to value"""
    VarzReceiver.VARZ_DATA[(metric, source)] = value
