import distutils
import shlex
import subprocess

from setuptools import setup, find_packages, Command


class PyInstallerCommand(Command):
    """
    setup.py command to run PyInstaller
    Usage:
      setup.py pyinstaller -o '--onefile ...'
    """
    description = "Run PyInstaller"
    user_options = [
        ('options=', 'o', 'Additional PyInstaller command line options'),
    ]

    def initialize_options(self):
        """Abstract method that is required to be overwritten"""
        self.options = []

    def finalize_options(self):
        """Abstract method that is required to be overwritten"""
        self.options = shlex.split(self.options or '')

    def run(self):
        command = ['pyinstaller', 'snowdrop.spec'] + self.options
        self.announce(
            'Running command: %s' % ' '.join(shlex.quote(x) for x in command),
            level=distutils.log.INFO)

        subprocess.check_call(command)


setup(
    name='snowdrop',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click~=6.7',
        'python-box~=3.2',
        'python-dateutil~=2.7',
        'requests~=2.19',
        'urllib3~=1.23',
        'yaspin~=0.12.0',
    ],
    entry_points='''
        [console_scripts]
        kx-sync=snowdrop.cli:cli
    ''',
    cmdclass={
        'pyinstaller': PyInstallerCommand,
    }
)
