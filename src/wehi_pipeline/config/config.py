'''
Created on 3Feb.,2017

@author: thomas.e
'''
import yaml
from wehi_pipeline.config.definition import ConfigDefinition
from wehi_pipeline.steps.jobStep import stepFactory
from wehi_pipeline.config import ConfigException
from wehi_pipeline.config.symbols import DestinationSymbol, BuiltInSymbol

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
            self.config = yaml.load(c)
        
    def validationErrors(self):
        return self.errors
    
    def isValid(self):
        return self.defn.isValid(self.config)
    
    def pathSymbolMap(self):
        return self._symbolMap('destination')
    
    def referenceSymbolMap(self):
        return self._symbolMap('reference')
            
    def _symbolMap(self, symbolType):
        paths = dict()
        
        for pathSymbol in self.config['pipeline-definition']['destinations']:
            name = pathSymbol['name']
            if name in paths:
                raise ConfigException(symbolType + ' ' + name + ' is multiply defined.')
            paths[name] = DestinationSymbol(name, pathSymbol['path'])    
            
        return paths
    
    def steps(self):
        steps = []
        
        for step in self.config['pipeline-definition']['steps']:
            steps.append(stepFactory(step))
            
        return steps

_builtin_symbol_map = {
    'sample' : BuiltInSymbol('sample'),
    'forward' : BuiltInSymbol('forward'),
    'backward' : BuiltInSymbol('backward'),
    'temp-dir' : BuiltInSymbol('temp-dir'),
    }

if __name__ == '__main__':
    c = Config('test-pipeline.yaml')
    
    if not c.isValid():
        print c.validationErrors()[0]
        
    steps = c.steps()
    print(steps)
        
    