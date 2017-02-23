'''
Created on 15Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import ConfigJobStep
from wehi_pipeline.config.symbols import resolveSymbols
from wehi_pipeline.toil_support.utils import execute

GRIDSS = '/home/thomas.e/mnWork/gridss-1.2.4-jar-with-dependencies.jar'
NP_THREADS = 8

class Gridss(ConfigJobStep):


    def __init__(self, config):
                    
        if 'name' not in config:
            config['name'] = 'trim'
            
        config['modules'] = [GRIDSS]
        
        super(Gridss, self).__init__(config)

    def function(self):
        def f(job, context):
    
            ram = 8 + 2 * NP_THREADS
            ram = '-Xmx' + str(ram) + 'g'

            cmd = 'java -ea ' + ram + ' -jar ' + GRIDSS + ' gridds.CallVariants' + \
                ' TMP_DIR=$temp-dir' + \
                ' REFERENCE=$hg38' + \
                ' INPUT=$sorted IC=1' + \
                ' OUTPUT=$vcf' + \
                ' THREADS=' + str(NP_THREADS)
            
            (cmd, outputFiles) = resolveSymbols(job, context, cmd, self.symbols())
            
            execute(context, cmd, outputFiles, modules=self.modules())
            
            return context
    
        return f
