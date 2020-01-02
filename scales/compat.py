
try:
  from cStringIO import StringIO as BytesIO
except ImportError:
  from io import BytesIO

try:
  Long = long
except NameError:
  Long = int
