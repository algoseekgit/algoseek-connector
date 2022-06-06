#!/usr/bin/env python
import os
import sys

from setuptools import setup
from typing import Dict

# 'setup.py publish' shortcut.
if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist bdist_wheel')
    os.system('twine upload dist/*')
    sys.exit()


about: Dict[str, str] = {}
with open("algoseek_connector/__version__.py") as fp:
    exec(fp.read(), about)
with open("README.md") as fp:
    readme = fp.read()


requires = [
    'clickhouse-driver'
]

setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__description__'],
    long_description=readme,
    long_description_content_type='text/markdown',
    author=about['__author__'],
    author_email=about['__author_email__'],
    packages=['algoseek_connector'],
    package_data={'': ['LICENSE', 'NOTICE']},
    package_dir={'algoseek_connector': 'algoseek_connector'},
    include_package_data=True,
    python_requires=">=3.6, <4",
    install_requires=requires,
    zip_safe=False
)
