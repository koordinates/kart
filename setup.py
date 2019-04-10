from setuptools import setup

setup(
    name='snowdrop',
    version='0.1',
    description='Koordinates gitlike experiments',
    url='http://github.com/koordinates/snowdrop',
    author='Koordinates Limited',
    author_email='support@koordinates.com',
    license='Proprietary',
    packages=['snowdrop'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'kxgit = snowdrop.cli:cli',
        ],
    },
)
