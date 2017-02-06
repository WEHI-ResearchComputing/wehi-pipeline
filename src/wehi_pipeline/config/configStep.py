'''
Created on 6Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.config import ConfigException

def stepFactory(config):
    stepType = config.keys()[0]
    stepValue = config[stepType]
     
    if stepType == 'generic':
        return GenericStep(stepValue)
     
    raise ConfigException('Unknown step type: ' + stepType)
        
class ConfigStep(object):

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


from wehi_pipeline.config.config import Symbol

class Output(Symbol):
    
    def __init__(self, config):
        super(Output, self).__init__(config['name'], config['destination'])
        self._type = config['type'] if 'type' in config else 'regular'
        
class PreCommand(Symbol):
    
    def __init__(self, config):
        super(PreCommand, self).__init__(config['variable'], None)
        self._command = config['command']

    
class GenericStep(ConfigStep):
    
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
            