from setuptools import setup, find_packages



requires = [
    'numpy>=1.15.1',
    'pandas>=0.23.4',
    'requests>=2.19.1',
    'scipy>=1.1.0'
]

setup(
    name='devodstoolkit',
    version='0.2.1',
    author='Nick Murphy',
    author_email='nick.murphy@devo.com',
    description='APIs for querying and loading data into Devo',
    url='https://github.com/devods/devodstoolkit',
    python_requires='>=3',
    install_requires=requires,
    packages=find_packages()
)
