import os
from setuptools import setup, find_packages

requires = []
console_scripts = [
    'reflector-cluster = ingester.main:main',
]
package_name = "ingester"
base_dir = os.path.abspath(os.path.dirname(__file__))

setup(
    name=package_name,
    version="0.0.1",
    author="LBRY Inc.",
    author_email="hello@lbry.io",
    license='MIT',
    packages=find_packages(base_dir, exclude=['tests']),
    install_requires=requires,
    entry_points={'console_scripts': console_scripts},
)
