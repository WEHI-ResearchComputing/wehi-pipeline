'''
Created on 22Nov.,2016

@author: thomas.e
'''

class WorkflowContext(object):
    '''
    This class transports context information between steps of an individual workflow
    '''


    def __init__(self, forward, backward, identifier, steps):
        self.forward = forward
        self.backward = backward
        self.identifier = identifier
        self.touchOnly = False
        self.steps = steps
        
    def setTouchOnly(self, touchOnly):
        self.touchOnly = touchOnly
        
    def touchOnly(self):
        return self.touchOnly