import os
from setuptools import setup
from setuptools import find_packages
import PyDB

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name = 'PyDB',
    version = PyDB.__version__,
    packages = find_packages(exclude=('tests', 'tests.*')),
    include_package_data=True,
)
