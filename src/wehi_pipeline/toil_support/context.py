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


    def __init__(self, forward, backward, identifier, tmpBase, steps):
        self.forward = forward
        self.backward = backward
        self.identifier = identifier
        self.tmpBase = tmpBase
        self.touchOnly = False
        self.steps = steps
        self.knownFiles = []
        
    def setTouchOnly(self, touchOnly):
        self.touchOnly = touchOnly
        
    def touchOnly(self):
        return self.touchOnly
    
    def getTempDir(self):
        p = os.path.join(self.tmpBase, self.identifier)
        makedir(p)
        return p
