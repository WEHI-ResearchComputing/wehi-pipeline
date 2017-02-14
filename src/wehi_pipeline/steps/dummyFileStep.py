'''
Created on 14Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import JobStep
from wehi_pipeline.toil_support.utils import execute
from wehi_pipeline.config.symbols import resolveSymbols


class DummyFile(JobStep):

    def __init__(self, config):
        super(DummyFile, self).__init__(config)
        
        self._fileName = config['file-name']
        self._content = config['content'] if 'content' in config else None
        
        
    def symbols(self):
        return None
    
    def function(self):
        def f(job):
            
            strings = [self._fileName]
            if self._content is not None:
                strings.append(self._content)
                
            (strings, _) = resolveSymbols(job, strings, None)
            
            if self._content == None:
                cmd = 'touch ' + strings[0]
                execute(cmd, cmd, None)
            else:
                cmd = 'echo ' + strings[1]
                execute(cmd, cmd, None, outfn=strings[0])
                
            print(strings[0] + ' ' + strings[1])
        
        return f