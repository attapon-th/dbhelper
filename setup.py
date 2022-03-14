#!/usr/bin/env python
import collections
from setuptools import setup, find_packages


ReqOpts = collections.namedtuple(
    'ReqOpts', ['skip_requirements_regex', 'default_vcs'])

opts = ReqOpts(None, 'git')

# version should use the format 'x.x.x' (instead of 'vx.x.x')
setup(
    name='dbhelper',
    version='0.2.4',
    description='',
    long_description="",
    long_description_content_type='text/markdown',
    author='Attapon Thanawong',
    author_email='attapon.srem@gmail.com',
    url='https://github.com/attapon/DbHelper',
    keywords="database vertica mysql parquet csv",
    packages=find_packages(),
    py_modules=["sql_parquet"],
    install_requires=[
        'dsnparse>=0.1.15',
        'pandas>=1.3.4',
        'vertica-python>=1.0.1',
        'click==8.0.3',
        'SQLAlchemy>=1.4.28',
        'pyarrow>=7.0.0',
        'mysql-connector-python>=8.0.27',
        'wheel',
    ])
