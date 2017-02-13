'''
Created on 3Feb.,2017

@author: thomas.e
'''
import yaml
from wehi_pipeline.config.definition import ConfigDefinition
from wehi_pipeline.steps.jobStep import stepFactory
from wehi_pipeline.config import ConfigException
from wehi_pipeline.config.symbols import DestinationSymbol, ReferenceSymbol
from toil.job import Job
from wehi_pipeline.toil_support.context import WorkflowContext

class Config(object):
    '''
    classdocs
    '''

    def __init__(self, fileName):
        '''
        Constructor
        '''
            
        self.defn = ConfigDefinition()
        
        with open(fileName) as c:
            config = yaml.load(c)
            
        if 'pipeline-definition' not in config:
            raise ConfigException('Configuration is not for a pipeline.')
        self.config = config['pipeline-definition']
        
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
                
        
    def validationError(self):
        return self.defn.validationError(self.config)
    
    def isValid(self):
        return self.defn.isValid(self.config)
    
    def pathSymbolMap(self):
        return self._pathSymbols
    
    def referenceSymbolMap(self):
        return self._referenceSymbols
    
    def builtinSymbolMap(self):
        return self._builtinSymbols
            
    def _symbolMapFor(self, symbolType, symbolConstructor):
        
        paths = dict()
        
        if symbolType not in self.config:
            return paths
        
        for symbol in self.config[symbolType]:
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
        steps = []
        
        for step in self.config['steps']:
            steps.append(stepFactory(step))
            
        return steps


if __name__ == '__main__':
    c = Config('test-pipeline.yaml')
    
    j = Job()
    j.context = WorkflowContext('forwardO', 'backwardO', 'sampleO', '/tmp', c.steps())
    
    c.steps()[0].function()(j)
    
    