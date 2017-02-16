'''
Created on 15Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import ConfigJobStep
from wehi_pipeline.config.symbols import resolveSymbols
from wehi_pipeline.toil_support.utils import execute

GRIDSS = 'gridss'
NP_THREADS = 8

class Gridss(ConfigJobStep):


    def __init__(self, config):
                    
        if 'name' not in config:
            config['name'] = 'trim'
            
        config['modules'] = [GRIDSS]
        
        super(Gridss, self).__init__(config)

    def function(self):
        def f(job):
    
            cmd = GRIDSS + ' au.edu.wehi.idsv.Idsv' + \
                ' TMP_DIR=$temp-dir' + \
                ' REFERENCE=$ref' + \
                ' INPUT=$sorted IC=1' + \
                ' OUTPUT=$vcf' + \
                ' THREADS=' + str(NP_THREADS)
            
            (cmd, outputFiles) = resolveSymbols(job, cmd, self.symbols(), self)
            
            execute(job, cmd, outputFiles)
    
        return f
