from setuptools import setup

setup(
  name='scales-rpc',
  version='1.0.4',
  author='Steve Niemitz',
  author_email='sniemitz@twitter.com',
  url='https://www.github.com/steveniemitz/scales',
  summary='A generic python RPC client framework.',
  packages=['scales',
            'scales.http',
            'scales.kafka',
            'scales.loadbalancer',
            'scales.mux',
            'scales.pool',
            'scales.redis',
            'scales.thrift',
            'scales.thrifthttp',
            'scales.thriftmux'],
  install_requires=[
      'gevent>=0.13.8',
      'thrift>=0.5.0',
      'kazoo>=1.3.1',
      'requests>=2.0.0']
)
