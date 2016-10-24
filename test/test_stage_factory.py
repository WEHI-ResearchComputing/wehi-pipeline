'''
Created on 12Oct.,2016

@author: thomas.e
'''

from wehi_pipeline.main import main
from wehi_pipeline.make_pipeline import make_pipeline


import unittest

PROGRAM_NAME = "ovarian_cancer_pipeline" 
PROGRAM_INFO = pkg_resources.require(PROGRAM_NAME)[0]
PROGRAM_VERSION = PROGRAM_INFO.version

class Test(unittest.TestCase):

    def testName(self):
        main(PROGRAM_NAME, PROGRAM_VERSION, make_pipeline)

if __name__ == "__main__":
    unittest.main()