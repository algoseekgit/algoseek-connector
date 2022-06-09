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
    'clickhouse-driver',
    'pandas'
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
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: SQL',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    include_package_data=True,
    python_requires=">=3.6, <4",
    install_requires=requires,
    zip_safe=False
)
