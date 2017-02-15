'''
Created on 15Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import JobStep
from wehi_pipeline.config.symbols import resolveSymbols, findOutputFile

GRIDSS = 'gridss'
NP_THREADS = 8

class Gridss(JobStep):


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
            
            print(cmd)
    
    #                 execute(job, cmd, outputFiles)
    
            
        
        return f
