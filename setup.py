import os
import sys

import setuptools

# Avoid polluting the .tar.gz with ._* files under Mac OS X
os.putenv('COPYFILE_DISABLE', 'true')

root = os.path.dirname(__file__)

description = "Hosting Based Interfacing"

with open(os.path.join(root, 'README'), encoding='utf-8') as f:
    long_description = '\n\n'.join(f.read().split('\n\n')[1:])

version = '??'
with open(os.path.join(root, 'hbi', 'version.py'), encoding='utf-8') as f:
    exec(f.read())

py_version = sys.version_info[:2]

if py_version < (3, 6):
    raise Exception("hbi requires Python >= 3.6.")

packages = ['hbi']

setuptools.setup(
    name='hbi',
    version=version,
    author='Compl Yue',
    author_email='',
    url='https://github.com/complyue/hbipy',
    description=description,
    long_description=long_description,
    download_url='https://pypi.python.org/pypi/hbi',
    packages=packages,
    extras_require={
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    platforms='all',
    license='BSD'
)
