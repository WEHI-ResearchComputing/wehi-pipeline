'''
Created on 22Nov.,2016

@author: thomas.e
'''

from wehi_pipeline.toil_support.utils import makedir
import os

class WorkflowContext(object):
    '''
    This class transports context information between steps of an individual workflow
    '''

    def __init__(self, forward, backward, identifier, tmpBase, symbols):
        self.forward = forward
        self.backward = backward
        self.identifier = identifier
        self.tmpBase = tmpBase
        self.touchOnly = False
        self.knownFiles = {}
        self.runSymbols = symbols
        
    def setTouchOnly(self, touchOnly):
        self.touchOnly = touchOnly
        
    def touchOnly(self):
        return self.touchOnly
    
    def getTempDir(self):
        p = os.path.join(self.tmpBase, self.identifier)
        makedir(p)
        return p

    def copy(self):
        c = WorkflowContext(self.forward, self.backward, self.identifier, self.tmpBase, self.runSymbols)
        c.setTouchOnly(self.touchOnly)
        c.knownFiles = self.knownFiles.copy()
        return c