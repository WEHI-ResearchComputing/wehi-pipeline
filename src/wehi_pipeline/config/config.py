'''
Created on 3Feb.,2017

@author: thomas.e
'''
import yaml
from wehi_pipeline.config.definition import ConfigDefinition

class Config(object):
    '''
    classdocs
    '''


    def __init__(self, fileName):
        '''
        Constructor
        '''
        
        with open(fileName) as c:
            self.config = yaml.load(c)
            
        defn = ConfigDefinition()
        defn.validate(self.config)
        
        
        
if __name__ == '__main__':
    c = Config('test-pipeline.yaml')
        