'''
Created on 16Feb.,2017

@author: thomas.e
'''

import re
import sys
import os
from wehi_pipeline.config import ConfigException

class Fastqs(object):

    def __init__(self, config):
        
        self._paired = config['paired'] if 'paired' in config else True
        
        self._path = config['path']
        
        self._pattern = config['pattern']
        
        self._startAt = int(config['start-at']) if 'start-at' in config else 0
        self._finishAt = int(config['finish-at']) if 'finish-at' in config else sys.maxint
        
        self._fastqs = dict()
        
        self._fastqs = dict()
        
        for fn in os.listdir(self._path):
            m = re.match(self._pattern, fn)
            if m is None:
                continue
            
            sample = m.group('sample')
            if sample is None:
                raise ConfigException('The fastqs file pattern does not have a sample group. Unique samples cannot be identified.')
            
            if sample in self._fastqs:
                fq = self._fastqs[sample]
            else:
                fq = Fastq(sample, self._path)
                self._fastqs[sample] = fq
            
            fq.addFile(fn)
            
    def fastqs(self):
        # Sort to ensure that subsets are deterministic
        fqs = self._fastqs.values()
        return sorted(fqs, key=lambda fq: fq.forward())[self._startAt:self._finishAt]
    
class Fastq(object):
    
    def __init__(self, sample, path):
        self._forward = None
        self._backward = None
        self._sample = sample
        self._path = path
        
    def addFile(self, fn):
        if self._forward == None:
            self._forward = fn
            return
        
        if fn < self._forward:
            self._backward = self._forward
            self._forward = fn
        else:
            self._backward = fn
            
    def forward(self):
        return os.path.join(self._path, self._forward)
    
    def backward(self):
        return os.path.join(self._path, self._backward)
    
    def sample(self):
        return self._sample
    
    def paired(self):
        return self._backward is not None
    
    