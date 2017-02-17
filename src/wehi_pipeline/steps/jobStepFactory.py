'''
Created on 14Feb.,2017

@author: thomas.e
'''

from wehi_pipeline.config import ConfigException
from wehi_pipeline.steps.genericStep import GenericStep
from wehi_pipeline.steps.dummyFileStep import DummyFile
from wehi_pipeline.steps.trimStep import Trim
from wehi_pipeline.steps.alignStep import Align
from wehi_pipeline.steps.sortStep import Sort
from wehi_pipeline.steps.gridssStep import Gridss

_step_registry = {
    'generic'    : GenericStep,
    'dummy-file' : DummyFile,
    'trim'       : Trim,
    'align'      : Align,
    'sort'       : Sort,
    'gridss'     : Gridss
    }

def stepFactory(stepConfig):
    stepType = stepConfig.keys()[0]
    stepValue = stepConfig[stepType]
         
    if stepType in _step_registry:
        return _step_registry[stepType](stepValue)
     
    raise ConfigException('Unknown step type: ' + stepType)
