'''
Created on 6Feb.,2017

@author: thomas.e
'''

from wehi_pipeline.config import ConfigException
from wehi_pipeline.toil_support.utils import asList
from wehi_pipeline.config.symbols import evaluate

def stepFactory(stepConfig):
    stepType = stepConfig.keys()[0]
    stepValue = stepConfig[stepType]
     
    if stepType == 'generic':
        return GenericStep(stepValue)
     
    raise ConfigException('Unknown step type: ' + stepType)

from wehi_pipeline.config.symbols import PreCommand        
from wehi_pipeline.config.symbols import Output

class JobStep(object):

    def __init__(self, config):
        self._config = config
        self._name = self._config['name']
        
        self._outputs = []
        if 'outputs' in self._config:
            outputs = self._config['outputs']
            for o in outputs:
                self._outputs.append(Output(o))
        
    def name(self):
        return self._name
        
    def outputs(self):
        return self._outputs

    def function(self):
        raise Exception('function is no implemented.')
    
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
            return dict()
        
    def function(self):
        def f(job):
            
            (cmds, outputFiles) = _prepareCommandLines(job, self._commands, self._config, self, job.context.knownFiles)
                
            print(cmds)
            print(outputFiles)
#             execute(job, cmds, outputFiles)
        return f

def _isEmpty(thing):
    if thing is None:
        return True
    if type(thing) is list:
        return list == []
    return False
    
def _prepareCommandLines(job, commands, config, stepConfig, knownFiles):
    
    if _isEmpty(commands):
        return (None, None)
    
    commands = asList(commands)
    
    processedCommands = []
    outFiles = []
    for cmd in commands:
        (processedCommand, fs) = _processCommandLine(job, cmd, config, stepConfig, knownFiles)
        processedCommands.append(processedCommand)
        outFiles = outFiles + fs
        
    return (processedCommand, outFiles)

def _processCommandLine(job, cmd, config, stepConfig, knownFiles):

    evaluate(job, cmd, config, stepConfig, knownFiles)

    outputFiles = []    
    for symbol in stepConfig.symbols():
        if type(symbol) is Output:
            outputFiles.append(symbol.pipeLineFile())
            knownFiles[symbol.name()] = symbol
            
            
    return (cmd, outputFiles)
