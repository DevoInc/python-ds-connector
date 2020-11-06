import pathlib
from setuptools import setup, find_packages

here = pathlib.Path(__file__).parent.absolute().resolve()
about = {}
with open(here / 'devodsconnector' / '__version__.py') as f:
    exec(f.read(), about)


requires = [
    'numpy>=1.15.1',
    'pandas>=0.23.4',
    'requests>=2.19.1',
    'scipy>=1.1.0',
    'devo-sdk>=3.4.1'
]

setup(
    name='devodsconnector',
    version=about['__version__'],
    author='Nick Murphy',
    author_email='nick.murphy@devo.com',
    license='MIT',
    classifiers=["License :: OSI Approved :: MIT License"],
    description='APIs for querying and loading data into Devo',
    url='https://github.com/DevoInc/python-ds-connector',
    python_requires='>=3.5',
    install_requires=requires,
    packages=find_packages()
)
