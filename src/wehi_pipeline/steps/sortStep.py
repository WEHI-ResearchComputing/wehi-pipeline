'''
Created on 15Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import JobStep
from wehi_pipeline.config.symbols import resolveSymbols, findOutputFile

SAM = 'sam'
NP_THREADS = 8


class Sort(JobStep):


    def __init__(self, config):
                    
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
        def f(job):
            
            sort = [
                SAM + ' view -b -@ ' + str(NP_THREADS) + ' $aligned', 
                SAM + ' sort -@ ' + str(NP_THREADS) + ' -'
                ]

            (sort, outputFiles) = resolveSymbols(job, sort, self.symbols(), self)
            
            print(sort)
            
            alignedFile = findOutputFile('aligned')
            sortedFile = findOutputFile('sorted')
            
            print(sortedFile.path())
            print(alignedFile.path())
            #                 execute(job, sort, outputFiles, outfn=sortedFile.path(), infn=alignedFile.path())
                
        return f
