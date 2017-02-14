'''
Created on 14Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.config import ConfigException
from wehi_pipeline.steps.genericStep import GenericStep
from wehi_pipeline.steps.dummyFileStep import DummyFile


_step_registry = {
    'generic' : GenericStep,
    'dummy-file' : DummyFile
    }

def stepFactory(stepConfig):
    stepType = stepConfig.keys()[0]
    stepValue = stepConfig[stepType]
         
    if stepType in _step_registry:
        return _step_registry[stepType](stepValue)
     
    raise ConfigException('Unknown step type: ' + stepType)
