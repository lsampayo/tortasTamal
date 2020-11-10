from _version import __version__
from setuptools import setup, find_packages

setup(
        name='data-input',
        version=__version__,
        author='Luis Enrique Marquez',
        author_email='luis.marquez@gmail.com',
        url=None,
        description='Tortas de Tamal Data Input',
        long_description='Tortas de Tamal Data Input',
        license='(c) Tortas de Tamal Inc 2020',
        python_requires='>=3.6',
        packages=find_packages()
)