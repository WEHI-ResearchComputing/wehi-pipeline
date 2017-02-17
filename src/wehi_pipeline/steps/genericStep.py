'''
Created on 6Feb.,2017

@author: thomas.e
'''

from wehi_pipeline.toil_support.utils import asList, execute

from wehi_pipeline.steps.jobStep import ConfigJobStep
from wehi_pipeline.config.symbols import PreCommand, resolveSymbols
    
class GenericStep(ConfigJobStep):
    
    def __init__(self, config):
        super(GenericStep, self).__init__(config)
        
        self._preCommands = []
        if 'precommands' in config:
            for preCommand in asList(config['precommands']):
                self._preCommands.append(PreCommand(preCommand))

        if 'modules' in config:
            self._modules = asList(config['modules'])
        else:
            self._modules = None
            
        self._commands = asList(config['commands'])
            
    def symbols(self):
        return self._outputs + self._preCommands
        
    def function(self):
        def f(job):
            
            (cmds, outputFiles) = resolveSymbols(job, self._commands, self.symbols(), self)
                
            execute(job, cmds, outputFiles, self._modules)
            
        return f

