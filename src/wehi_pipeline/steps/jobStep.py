'''
Created on 14Feb.,2017

@author: thomas.e
'''

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
        