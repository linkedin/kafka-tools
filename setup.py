import subprocess
import os

from setuptools import setup, find_packages, Command
from codecs import open

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


install_requires = ['pip>=8.1.2', 'paramiko', 'kazoo', 'pex', 'wheel']

entry_points = {
    'console_scripts': [
        'kafka-assigner=kafka.tools.assigner:main',
    ],
}


class VirtualEnv(Command):
    user_options = [
        ('python-interpreter=', 'p', 'The interpreter to build your venv with')
    ]

    def initialize_options(self):
        if os.environ.get('VIRTUAL_ENV'):
            raise RuntimeError(
                "Please deacivate your current virtual environment before running this subcommand!"
            )
        self.python_interpreter = 'python'

    def finalize_options(self):
        print("Using interpreter: {0}".format(self.python_interpreter))

    def run(self):
        venv_path = os.path.join(here, 'venv', 'kafka-tools')
        venv_cmd = [
            'virtualenv',
            '-p', self.python_interpreter,
            venv_path
        ]
            venv_path
        ]
        print('Creating virtual environment in ', venv_path)
        subprocess.check_call(venv_cmd)
        print(
            'Linking `activate` to top level of project.\n'
            'To activate, simply run `source activate`.'
        )
        try:
            os.symlink(
                os.path.join(venv_path, 'bin', 'activate'),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), 'activate')
            )
        except OSError:
            print(
                'Unable to create symlink, you may have a stale symlink from a previous invocation, '
                'or you may have already set up your virtual environment.'
            )


class Pexify(Command):
    user_options = []

    def initialize_options(self):
        """Abstract method that is required to be overwritten"""

    def finalize_options(self):
        """Abstract method that is required to be overwritten"""

    def run(self):
        if not os.path.exists('dist/wheel-cache'):
            print(
                'You need to create dist/wheel-cache first! You\'ll need to run the following:\n'
                '    mkdir -p dist/wheel-cache\n'
                '    pip wheel -w dist/wheel-cache {0}'.format(' '.join(install_requires))
            )
            return
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
    install_requires=install_requires,

    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },

    entry_points=entry_points,

    cmdclass={
        'virtualenv': VirtualEnv,
        'pexify': Pexify,
    },
)
