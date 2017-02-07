'''
Created on 6Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.config import ConfigException
from wehi_pipeline.toil_support.utils import execute

def stepFactory(config):
    stepType = config.keys()[0]
    stepValue = config[stepType]
     
    if stepType == 'generic':
        return GenericStep(stepValue)
     
    raise ConfigException('Unknown step type: ' + stepType)

from wehi_pipeline.config.symbols import PreCommand        
from wehi_pipeline.config.symbols import Output

class JobStep(object):

    def __init__(self, name, outputs):
        self._name = name
        self._outputs = []
        if outputs is not None:
            if type(outputs) is not list:
                outputs = [outputs]
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
        outputs = config['outputs'] if 'outputs' in config else None
        super(GenericStep, self).__init__(config['name'], outputs)
        
        self._preCommands = []
        if 'precommands' in config:
            for preCommand in config['precommands']:
                self._preCommands.append(PreCommand(preCommand))

        if 'modules' in config:
            self._modules = config['modules']
        else:
            self._modules = []
            
    def function(self):
        def f(job):
            
            _resolveSymbols
            precommandValues = _resolvePrecommands(self._preCommands)
            
            outputFiles = _prepareOutputs(self._outputs, precommandValues)
            
            cmds = _prepareCommandLines(self._commands, precommandValues, outputFiles)
            
            execute(job, cmds, outputFiles)
            
        return f

def _isEmpty(thing):
    if thing is None:
        return True
    if type(thing) is list:
        return list == []
    return False

def _resolvePrecommands(preCommands):
    if _isEmpty(preCommands):
        return None

    resolvedCommands = []
    for preCommand in preCommand:
        
    pu = subprocess.check_output("zcat " + forward + " | head -n1 | cut -d\":\" -f3,6,7 | sed 's/:/./g' | sed 's/\\s1//g'", shell=True)
    pu = pu.strip()
    
            
def _prepareOutputs(oututFiles, preCommands):
    pass
            
def _prepareCommandLines(commands, preCommands, oututFiles):
    pass
            