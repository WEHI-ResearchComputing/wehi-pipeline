'''
Created on 3Feb.,2017

@author: thomas.e
'''

import yaml
from jsonschema.validators import Draft4Validator
from jsonschema.validators import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError

DEFINITION = 'config-definition.yaml'


class ConfigDefinition(object):
    '''
    This reads the DEFINITION which defines a valid pipeline configuration
    '''


    def __init__(self):
        with open(DEFINITION, 'r') as y:
            self.defn = yaml.load(y)
            
        # This will throw an error if the schema itself is invalid otherwise it just validate everything.
        try:
            validate([], self.defn)
        except SchemaError:
            raise
#             raise Exception('The pipeline configuration schema is invalid. Contact Research Computing.')
        except ValidationError:
            pass
        
        self.validator = Draft4Validator(self.defn)
        

    def isValid(self, config):
        return self.validator.is_valid(config)
    
    def validationErrors(self, config):
        return sorted(self.validator.iter_errors(config), key=str)
