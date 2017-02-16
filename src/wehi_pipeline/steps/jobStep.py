'''
Created on 14Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.config.symbols import Output
import logging


class ConfigJobStep(object):

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
    
    def symbols(self):
        return self._outputs

    def function(self):
        raise Exception('function is not implemented.')
    
    def _nextStep(self):
        
        steps = self._config.steps()
        n = len(steps)
        for i in range(n):
            if steps[i].name() == self._name:
                if i < n-1:
                    return steps[i]
                
        return None
        
    def wrappedFunction(self):
        
        def wf(job):
            self.function()(job)
            
            logging.info('Finished step:' + str(self._name))

            nextStep = self._nextStep()
            if nextStep == None:
                return

            logging.info('Launching step: ' + nextStep.name())
            nj = job.addChildJobFn(self.wrappedFunction(nextStep.function()))
            nj.context = job.context
            
            
        return wf