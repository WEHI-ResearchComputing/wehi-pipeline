'''
Created on 22Nov.,2016

@author: thomas.e
'''

class JobStep(object):
    '''
    classdocs
    '''

    def __init__(self, function, children, followers=None):
        '''
        Constructor
        '''
        self.function = function
        self.children = _emptyToNone(children)
        self.followers = _emptyToNone(followers)
        
        
def _emptyToNone(l):
    if l is None:
        return None
    if type(l) is not list:
        return [l]
    if len(l) == 0:
        return None
    return l

def followersOf(step, steps):
    if step is None:
        return None
    
    if steps is None:
        return None
    
    for t in steps:
        if t.function == step:
            return t.followers
        
    return None;
 
def childrenOf(step, steps):
    if step is None:
        return None
    
    if steps is None:
        return None
    
    for t in steps:
        if t.function == step:
            return t.children
        
    return None;
