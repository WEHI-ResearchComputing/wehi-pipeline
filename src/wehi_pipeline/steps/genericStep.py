'''
Created on 6Feb.,2017

@author: thomas.e
'''

from wehi_pipeline.toil_support.utils import asList
from wehi_pipeline.config.symbols import resolveSymbols

from wehi_pipeline.config.symbols import PreCommand        
from wehi_pipeline.steps.jobStep import JobStep
    
class GenericStep(JobStep):
    
    def __init__(self, config):
        super(GenericStep, self).__init__(config)
        
        self._preCommands = []
        if 'precommands' in config:
            for preCommand in asList(config['precommands']):
                self._preCommands.append(PreCommand(preCommand))

        if 'modules' in config:
            self._modules = asList(config['modules'])
        else:
            self._modules = []
            
        self._commands = asList(config['commands'])
            
    def symbols(self):
        return self._outputs + self._preCommands
        
    def function(self):
        def f(job):
            
            (cmds, outputFiles) = resolveSymbols(job, self._commands, self.symbols(), self)
                
            print(cmds)
            print(outputFiles)
            #             execute(job, cmds, outputFiles)
            
        return f

