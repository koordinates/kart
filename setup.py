from setuptools import setup

setup(
    name='snowdrop',
    version='0.1',
    description='Distributed version-control for datasets',
    url='http://github.com/koordinates/snowdrop',
    author='Koordinates Limited',
    author_email='support@koordinates.com',
    license='Proprietary',
    packages=['snowdrop'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'snow = snowdrop.cli:cli',
            'snowdrop = snowdrop.cli:cli',
        ],
    },
)
