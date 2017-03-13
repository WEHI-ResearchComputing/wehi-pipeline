'''
Created on 14Feb.,2017

@author: thomas.e
'''

from wehi_pipeline.toil_support.utils import asList
from wehi_pipeline.config.symbols import Output
import logging
import dill

class ConfigJobStep(object):

    def __init__(self, config):
        self._config = config
        self._name = self._config['name']
        
        self._outputs = []
        if 'outputs' in self._config:
            outputs = self._config['outputs']
            for o in outputs:
                self._outputs.append(Output(o))

        if 'modules' in config:
            self._modules = asList(config['modules'])
        else:
            self._modules = []
                
    def name(self):
        return self._name
        
    def outputs(self):
        return self._outputs
    
    def symbols(self):
        return self._outputs

    def function(self):
        raise Exception('function is not implemented.')
    
    def numRecommendedThreads(self):
        raise Exception('function is not implemented.')
    
    def recommendedMemory(self):
        raise Exception('function is not implemented.')
    
    def setNextStep(self, step):
        self._nextStep = step
        
    def nextStep(self):
        return self._nextStep
    
    def config(self):
        return {self._stepType: self._config}
    
    def modules(self):
        return self._modules
        
def wehiWrapper(job, step=None, context=None):
    # Need to serialise and deserialise ourselves 
    # otherwise cPickle chokes
    context = dill.loads(context)
    
    logging.info('Starting step:' + step.name())
    context = step.function()(job, context)
    logging.info('Finished step:' + step.name())

    nextStep = step.nextStep()
    if nextStep == None:
        return
     
    logging.info('Launching step: ' + nextStep.name())
    
    dillContext = dill.dumps(context)
    
    job.addChildJobFn(wehiWrapper, step=nextStep, context=dillContext)
    
    
