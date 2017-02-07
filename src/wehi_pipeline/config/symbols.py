'''
Created on 7Feb.,2017

@author: thomas.e
'''

        
class Symbol(object):
    
    def __init__(self, name, value, resolved):  
        self._name = name
        self._value = value
        self._resolved = resolved
        
    def setValue(self, value):
        self._value = value
    
    def isResolved(self):
        return self._resolved
    
    def name(self):
        return self._name
    
    def value(self):
        return self._value
    
class BuiltInSymbol(Symbol):
    
    def __init__(self, name):
        super(BuiltInSymbol, self).__init__(name, None, False)
    
class DestinationSymbol(Symbol):
    
    def __init__(self, name, path):
        super(DestinationSymbol, self).__init__(name, path, True)
    
class ReferenceSymbol(Symbol):
    
    def __init__(self, name, path):
        super(ReferenceSymbol, self).__init__(name, path, True)

class Output(Symbol):
    
    def __init__(self, config):
        super(Output, self).__init__(config['name'], config['destination'], True)
        self._type = config['type'] if 'type' in config else 'regular'
        
class PreCommand(Symbol):
    
    def __init__(self, config):
        super(PreCommand, self).__init__(config['variable'], None, False)
        self._command = config['command']

