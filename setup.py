#!/usr/bin/env python

from setuptools import setup

setup(
    name='wehi_pipeline',
    version='0.0.1',
    author='Evan Thomas',
    author_email='thome.e@wehi.edu.au',
    packages=['src'],
    entry_points={
        'console_scripts': ['wehi_pipeline = src.main:main']
    },
    url='https://github.com/evan-wehi/wehi-pipeline',
    license='LICENSE',
    description='wehi_pipeline is a Ruffus wrapper for running bioinformatics pipelines at The Walter and Eliza Hall Institute', 
    long_description=open('README.md').read(),
    install_requires=[
        "ruffus >= 2.6.3",
        "pipeline_base >= 1.0.0"
    ],
)
