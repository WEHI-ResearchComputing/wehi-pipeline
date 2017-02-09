'''
Created on 3Feb.,2017

@author: thomas.e
'''
import yaml
from wehi_pipeline.config.definition import ConfigDefinition
from wehi_pipeline.steps.jobStep import stepFactory
from wehi_pipeline.config import ConfigException
from wehi_pipeline.config.symbols import DestinationSymbol, BuiltInSymbol, ReferenceSymbol

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
            
        self._pathSymbols = self._symbolMapFor('destinations', DestinationSymbol)
        self._referenceSymbols = self._symbolMapFor('references', ReferenceSymbol)
        self._builtinSymbols = _builtin_symbol_map.copy()
        
        sns = set()
        for sn in self._pathSymbols.keys():
            if sn in sns:
                raise ConfigException('The symbol ' + sn + ' is multiply defined.')
            sns.add(sn)
                
        
    def validationErrors(self):
        return self.errors
    
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
        
        for symbol in self.config['pipeline-definition'][symbolType]:
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
        
        for step in self.config['pipeline-definition']['steps']:
            steps.append(stepFactory(step))
            
        return steps

_builtin_symbol_map = {
    'sample'   : BuiltInSymbol('sample'),
    'forward'  : BuiltInSymbol('forward'),
    'backward' : BuiltInSymbol('backward'),
    'temp-dir' : BuiltInSymbol('temp-dir'),
    }

if __name__ == '__main__':
    c = Config('test-pipeline.yaml')
    
    if not c.isValid():
        print c.validationErrors()[0]
        
    steps = c.steps()
    print(steps)
        
    