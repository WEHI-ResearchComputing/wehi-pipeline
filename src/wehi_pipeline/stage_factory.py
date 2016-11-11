'''
Created on 14Oct.,2016

@author: thomas.e
'''

import stages

__default_registry__ = {'trimmomatic', stages.trimmomatic_stage_factory.factory}

def registry(extra_factories=None):
    d = __default_registry__.copy()
    
    if extra_factories is None:
        return d
    
    return d.update(extra_factories)
    
    