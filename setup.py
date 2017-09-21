from distutils.core import setup
from setuptools import find_packages

setup(
    name='wehi-pipeline',
    version='0.1',
    packages=['src.wehi_pipeline', 'src.wehi_pipeline.batchSystems'],
    url='https://github.com/WEHI-ResearchComputing/wehi-pipeline',
    license='GPLv3',
    author='Evan Thomas',
    author_email='thomas.e@wehi.edu.au',
    description='Wrappers and utilities to run CWL/Toil pipelines on WEHI infrastructure',
    packages=find_packages(where='src'),
    entry_points={'console_scripts': ['cwlwehi = wehi_pipeline.cwlwehi:main']}
)
