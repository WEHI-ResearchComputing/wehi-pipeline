'''
Created on 3Feb.,2017

@author: thomas.e
'''

import yaml

DEFINITION = 'config-definition.yaml'


class ConfigDefinition(object):
    '''
    This reads the DEFINITION which defines a valid pipeline configuration
    '''


    def __init__(self):
        with open(DEFINITION, 'r') as y:
            self.defn = yaml.load(y)
            
        print self.defn
        print self.defn['type']
        print self.defn['required']
        print self.defn['optional']['description']
        
        
        
        
        
if __name__ == '__main__':
    d = ConfigDefinition()