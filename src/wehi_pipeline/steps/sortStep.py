'''
Created on 15Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import ConfigJobStep
from wehi_pipeline.config.symbols import resolveSymbols, findOutputFile
from wehi_pipeline.toil_support.utils import execute

SAM = 'samtools'
NP_THREADS = 8


class Sort(ConfigJobStep):


    def __init__(self, config):
        
        if config is None:
            config = dict()
                    
        if 'name' not in config:
            config['name'] = 'trim'
            
        config['modules'] = ['samtools']
        
        config['outputs'] = [
            {
            'name' :  'sorted',
            'destination' : '$temp-dir',
            }
                   ]
        
        super(Sort, self).__init__(config)

    def function(self):
        def f(job, context):
            
            sort = [
                SAM + ' view -b -@ ' + str(NP_THREADS) + ' $aligned', 
                SAM + ' sort -@ ' + str(NP_THREADS) + ' -'
                ]

            (sort, outputFiles) = resolveSymbols(job, context, sort, self.symbols())
            
            alignedFile = findOutputFile(context, 'aligned')
            sortedFile = findOutputFile(context, 'sorted')
            
            execute(context, sort, outputFiles, outfn=sortedFile.path(), infn=alignedFile.path(), modules=self.modules())
            
            return context
                
        return f
