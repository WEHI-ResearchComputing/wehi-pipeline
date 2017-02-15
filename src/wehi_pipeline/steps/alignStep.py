'''
Created on 15Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import JobStep
from wehi_pipeline.config.symbols import resolveSymbols, findOutputFile

BWA = 'bwa'
NP_THREADS = 8

class Align(JobStep):

    def __init__(self, config):
            
        if 'name' not in config:
            config['name'] = 'align'
            
        config['modules'] = [BWA]
        
        config['outputs'] = [
            {
            'name' :  'aligned',
            'destination' : '$temp-dir',
            }
                   ]
        
        super(Align, self).__init__(config)
        
    def function(self):
        def f(job):
            
            cmd = BWA + ' mem -t ' + str(NP_THREADS) + ' $ref $forward $backward'
            (cmd, outputFiles) = resolveSymbols(job, cmd, self.symbols(), self)
            
            print(cmd)
            
            alignedFile = findOutputFile(job, 'aligned')
            print(alignedFile.path())
            
            # execute(job, cmd, outputFiles, outfn=alignedFile.path()) 
            
        return f
            