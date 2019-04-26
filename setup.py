import glob
import os.path
import sys

import setuptools

# Avoid polluting the .tar.gz with ._* files under Mac OS X
os.putenv("COPYFILE_DISABLE", "true")

root = os.path.dirname(__file__)

description = "Hosting Based Interfacing"

with open(os.path.join(root, "README"), encoding="utf-8") as f:
    long_description = "\n\n".join(f.read().split("\n\n")[1:])

version = "??"
with open(os.path.join(root, "hbi", "version.py"), encoding="utf-8") as f:
    exec(f.read())

py_version = sys.version_info[:2]

if py_version < (3, 7):
    raise Exception("hbi requires Python >= 3.7.")

packages = ["hbi"]

all_files_by_dir = {}
for f in glob.glob("hbi/**/*.py", recursive=True):
    d, n = os.path.dirname(f), f  # os.path.basename(f)
    if d in all_files_by_dir:
        all_files_by_dir[d].append(n)
    else:
        all_files_by_dir[d] = [n]
data_files = list(all_files_by_dir.items())

setuptools.setup(
    name="hbi",
    version=version,
    author="Compl Yue",
    author_email="",
    url="https://github.com/complyue/hbipy",
    description=description,
    long_description=long_description,
    download_url="https://pypi.python.org/pypi/hbi",
    packages=packages,
    data_files=data_files,
    extras_require={},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    platforms="all",
    license="BSD",
)
