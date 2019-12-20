from setuptools import setup, find_packages

setup(
    name='sno',
    version='0.2.0',
    description='Distributed version-control for datasets',
    url='http://github.com/koordinates/sno',
    author='Koordinates Limited',
    author_email='support@koordinates.com',
    license='Proprietary',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'sno = sno.cli:cli',
        ],
    },
)
