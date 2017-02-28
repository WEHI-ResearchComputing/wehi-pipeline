'''
Created on 15Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import ConfigJobStep
from wehi_pipeline.config.symbols import resolveSymbols, findOutputFile
from wehi_pipeline.toil_support.utils import execute

BWA = 'bwa'
NP_THREADS = 8

class Align(ConfigJobStep):

    def __init__(self, config):
            
        if 'name' not in config:
            config['name'] = 'align'
            
        config['modules'] = [BWA]
        
        config['outputs'] = [
            {
            'name' :  'aligned',
            'destination' : '$temp-dir'
            }
                   ]
        
        self._reference = config['reference']
        
        super(Align, self).__init__(config)
        
    def function(self):
        def f(job, context):
            
            cmd = BWA + ' mem -t ' + str(NP_THREADS) + ' ' + self._reference + ' $forward $backward'
            (cmd, outputFiles) = resolveSymbols(job, context, cmd, self.symbols())
            
            alignedFile = findOutputFile(context, 'aligned')
            
            execute(context, cmd, outputFiles, outfn=alignedFile.path(), modules=self.modules())
            
            return context
            
        return f
            