from distutils.core import setup

setup(
  name='scales',
  version='1.0.0',
  author='Steve Niemitz',
  author_email='sniemitz@twitter.com',
  url='https://www.github.com/steveniemitz/scales',
  packages=['scales',
            'scales.http',
            'scales.loadbalancer',
            'scales.pool',
            'scales.thrift',
            'scales.thrifthttp',
            'scales.thriftmux'],
  requires=['gevent (>0.13.8)', 'thrift (>0.5.0)']
)
