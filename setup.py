import os
from setuptools import setup, find_packages

requires = [
    'twisted==16.6.0',
    'pycrypto==2.6.1',
    'rq==0.8.0',
    'pyyaml==3.12'
]
console_scripts = [
    'prism-server = prism.server:main',
    'prism-supervisor = prism.supervisor:main'
]
package_name = "prism"
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
