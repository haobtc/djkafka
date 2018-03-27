from distutils.core import setup
from setuptools import find_packages

setup(name='djkafka',
      version='0.1.0',
      description='simple wrapper of kakfa-python in django',
      author='Zeng Ke',
      author_email='superisaac.ke@gmail.com',
      packages=find_packages(),
      install_requires=[
          'django >= 0.10.0',
          'msgpack >= 0.5.6',
          'kafka-python >= 1.4.2',
      ]
)
