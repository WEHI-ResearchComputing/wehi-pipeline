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
            
        defn = ConfigDefinition()
        
        with open(fileName) as c:
            config = yaml.load(c)
            
        if 'pipeline-definition' not in config:
            raise ConfigException('Configuration is not for a pipeline.')
        self._config = config['pipeline-definition']
        
        if not defn.isValid(self._config):
            raise ConfigException('Config is not valid: ' + defn.validationError())
            
        self._pathSymbols = self._symbolMapFor('destinations', DestinationSymbol)
        self._referenceSymbols = self._symbolMapFor('references', ReferenceSymbol)
        
        symbols = self.symbols()
        sns = set()
        for symbol in symbols:
            sn = symbol.name()
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
    
    def pathSymbols(self):
        return self._pathSymbols
    
    def referenceSymbols(self):
        return self._referenceSymbols
    
    def _symbolMapFor(self, symbolType, symbolConstructor):
        
        paths = []
        
        if symbolType not in self._config:
            return paths
        
        for symbol in self._config[symbolType]:
            name = symbol['name']
            paths.append(symbolConstructor(name, symbol['path']))    
            
        return paths
    
    def symbols(self):
        return self.pathSymbols() + self.referenceSymbols()
            
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
    