from setuptools import setup

setup(
    name='wehi-pipeline',
    version='0.1',
    packages=['wehi_pipeline', 'wehi_pipeline.batchSystems'],
    url='https://github.com/WEHI-ResearchComputing/wehi-pipeline',
    license='GPLv3',
    author='Evan Thomas',
    author_email='thomas.e@wehi.edu.au',
    description='Wrappers and utilities to run CWL/Toil pipelines on WEHI infrastructure',
    install_requires=[
    	'toil',
    	'toil[cwl]',
    	'cwltool',
    	'drmaa',
    	'html5lib',
    ],
    entry_points={'console_scripts': ['cwlwehi = wehi_pipeline.cwlwehi:main']}
)
