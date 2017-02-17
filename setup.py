from setuptools import setup, find_packages

setup(name='autoscaler',
      version='0.0.1',
      packages=find_packages(),
      install_requires=[
          'pykube',
          'requests',
          'ipdb',
          'boto',
          'boto3',
          'botocore',
          'click',
      ]
)

