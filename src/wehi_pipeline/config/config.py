'''
Created on 3Feb.,2017

@author: thomas.e
'''
import yaml
from wehi_pipeline.config.definition import ConfigDefinition
from wehi_pipeline.config import ConfigException
from wehi_pipeline.config.symbols import DestinationSymbol, ReferenceSymbol
from toil.job import Job
from wehi_pipeline.toil_support.context import WorkflowContext
from wehi_pipeline.steps.jobStepFactory import stepFactory
from wehi_pipeline.config.fastqs import Fastqs

class Config(object):
    '''
    classdocs
    '''

    def __init__(self, fileName):
        '''
        Constructor
        '''
            
        self._defn = ConfigDefinition()
        
        with open(fileName) as c:
            config = yaml.load(c)
            
        if 'pipeline-definition' not in config:
            raise ConfigException('Configuration is not for a pipeline.')
        self._config = config['pipeline-definition']
        
        if not self.isValid():
            raise ConfigException('Config is not valid: ' + self.validationError())
            
        self._pathSymbols = self._symbolMapFor('destinations', DestinationSymbol)
        self._referenceSymbols = self._symbolMapFor('references', ReferenceSymbol)
        
        if self._pathSymbols is not None:
            sns = set()
            for sn in self._pathSymbols.keys():
                if sn in sns:
                    raise ConfigException('The symbol ' + sn + ' is multiply defined.')
                sns.add(sn)
                
        self._fastqs = Fastqs(self._config['fastqs'])        

        self._steps = []
        
        previousStep = None
        for step in self._config['steps']:
            configStep = stepFactory(step)
            self._steps.append(configStep)
            if previousStep is not None:
                previousStep.setNextStep(configStep)
            previousStep = configStep
            
                
    def fastqs(self):
        return self._fastqs.fastqs()
        
    def name(self):
        return self._config['name']
    
    def description(self):
        return self._config['description'] if 'description' in self._config else None
    
    def validationError(self):
        return self._defn.validationError(self._config)
    
    def isValid(self):
        return self._defn.isValid(self._config)
    
    def pathSymbolMap(self):
        return self._pathSymbols
    
    def referenceSymbolMap(self):
        return self._referenceSymbols
    
    def builtinSymbolMap(self):
        return self._builtinSymbols
            
    def _symbolMapFor(self, symbolType, symbolConstructor):
        
        paths = dict()
        
        if symbolType not in self._config:
            return paths
        
        for symbol in self._config[symbolType]:
            name = symbol['name']
            if name in paths:
                raise ConfigException(symbolType + ' ' + name + ' is multiply defined.')
            paths[name] = symbolConstructor(name, symbol['path'])    
            
        return paths
    
    def symbols(self):
        x = dict()
        x.update(self.pathSymbolMap())
        x.update(self.referenceSymbolMap())
        x.update(self.buildinSymbolMap())
        
        return x
    
    def steps(self):
        return self._steps

if __name__ == '__main__':
    c = Config('../../../test/configs/mn.yaml')
    
    j = Job()
    j.context = WorkflowContext('forwardO', 'backwardO', 'sampleO', '/tmp', c.steps())
    
    for step in c.steps():
        print(step.name())
        step.function()(j)
        print('\n')
    