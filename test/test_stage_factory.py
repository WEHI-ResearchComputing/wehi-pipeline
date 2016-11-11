'''
Created on 12Oct.,2016

@author: thomas.e
'''

from wehi_pipeline.main import main
from wehi_pipeline.make_pipeline import make_pipeline
from pkg_resources import require

from sys import argv

import unittest

PROGRAM_NAME = "wehi_pipeline" 
# PROGRAM_INFO = require(PROGRAM_NAME)[0]
PROGRAM_VERSION = '0.0.1' #PROGRAM_INFO.version

class Test(unittest.TestCase):

    def testName(self):
        main(PROGRAM_NAME, PROGRAM_VERSION, make_pipeline)

if __name__ == "__main__":
    argv[1] = '--describe'
    unittest.main()