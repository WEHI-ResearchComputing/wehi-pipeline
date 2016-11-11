'''
Created on 24Oct.,2016

@author: thomas.e
'''

from wehi_pipeline.main import main
from pkg_resources import require 
from wehi_pipeline import make_pipeline

PROGRAM_NAME = "wehi_pipeline" 
# PROGRAM_INFO = require(PROGRAM_NAME)[0]
PROGRAM_VERSION = '0.0.1' #PROGRAM_INFO.version

main(PROGRAM_NAME, PROGRAM_VERSION, make_pipeline)
