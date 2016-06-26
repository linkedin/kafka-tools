import os
import subprocess
from codecs import open
from setuptools import setup, find_packages, Command


entry_points={
    'console_scripts': [
        'kafka-assigner = kafka.tools.assigner.__main__:main',
    ],
}

class Pex(Command):
  user_options = []

  def initialize_options(self):
    """Abstract method that is required to be overwritten"""

  def finalize_options(self):
    """Abstract method that is required to be overwritten"""

  def run(self):
    if not os.path.exists('dist/wheel-cache'):
      print('Creating dist/wheel-cache')
      os.makedirs('dist/wheel-cache')

    print('Building wheels')
    subprocess.check_call(['pip', 'wheel', '-w', 'dist/wheel-cache', '.'])

    for entry in entry_points['console_scripts']:
      name, call = tuple([_.strip() for _ in entry.split('=')])
      print('Creating {0} as {1}'.format(name, call))
      pex_cmd = [
        'pex',
        '--no-pypi',
        '--repo=dist/wheel-cache',
        '-o', 'build/bin/{0}'.format(name),
        '-e', call,
        '.',
      ]
      print('Running {0}'.format(' '.join(pex_cmd)))
      subprocess.check_call(pex_cmd)


here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='kafka-tools',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.0.1',

    author='Todd Palino',
    author_email='tpalino@linkedin.com',

    description='A collection of tools and scripts for working with Apache Kafka',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/linkedin/kafka-tools',

    # Choose your license
    license='Apache 2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'Topic :: Utilities',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='kafka tools admin',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=[
        'paramiko',
        'kazoo'
    ],
    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },
    entry_points=entry_points,
    cmdclass={
        'pexify': Pex
    },

)
