#!/usr/bin/env python
import os
from setuptools import setup, find_packages


with open("README.MD", "r", encoding="utf-8", errors="ignore") as fh:
    long_description = fh.read()


def list_files(directory):
    paths = []
    for path, directories, filenames in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join(path, filename))
    return paths


__version__ = "0.7.5"
setup(
    name="dbhelper",
    version=__version__,
    author="attapon.th",
    maintainer="attapon.th",
    maintainer_email="attapon.4work@gmial.com",
    url="https://github.com/attapon-th/dbhelper",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[x.strip() for x in open("requirements.txt").readlines()],
    packages=find_packages(),
    python_requires=">=3.8",
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points={
        "console_scripts": ["dbper=dbper:main"],
    },
    py_modules=["dbper"],
    # package_data={"sqlprocess": list_files("sqlprocess") + ["requirements.txt", "vprocess.py"]},
    # include_package_data=True,
)
