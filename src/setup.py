import os
import sys
import platform
from setuptools import setup, find_packages
from distutils.core import Extension

setup(name = 'pyg-cell', version = '0.0.43', packages = find_packages(), python_requires = '>=3.6.')