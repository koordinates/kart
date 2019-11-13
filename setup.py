from setuptools import setup, find_packages

setup(
    name='sno',
    version='0.2.0rc1',
    description='Distributed version-control for datasets',
    url='http://github.com/koordinates/sno',
    author='Koordinates Limited',
    author_email='support@koordinates.com',
    license='Proprietary',
    packages=find_packages(),
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'sno = sno.cli:cli',
        ],
    },
)
