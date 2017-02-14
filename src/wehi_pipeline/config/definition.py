'''
Created on 3Feb.,2017

@author: thomas.e
'''

import yaml
from jsonschema.validators import Draft4Validator
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from wehi_pipeline.config import ConfigException

DEFINITION = 'config-definition.yaml'

class ConfigDefinition(object):
    '''
    This reads the DEFINITION which defines a valid pipeline configuration
    '''

    def __init__(self):
        with open(DEFINITION, 'r') as y:
            defn = yaml.load(y)
            
        try:
            self.defn = defn['definitions']['pipeline-definition']
        except KeyError:
            raise ConfigException('The configuration definition is not valid')
         
        # This will throw an error if the schema itself is invalid otherwise it just validate everything.
        try:
            Draft4Validator.check_schema(self.defn)
        except SchemaError:
            raise
#             raise Exception('The pipeline configuration schema is invalid. Contact Research Computing.')
        except ValidationError:
            raise
        
        self.validator = Draft4Validator(self.defn)
        

    def isValid(self, config):
        return self.validator.is_valid(config)
    
    def validationError(self, config):
        errors = sorted(self.validator.iter_errors(config), key=str)
        if errors is None or len(errors) == 0:
            return ''
        return str(errors[0])
        
