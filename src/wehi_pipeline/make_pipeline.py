'''
Created on 14Oct.,2016

@author: thomas.e
'''
from ruffus import Pipeline, suffix, formatter, add_inputs, output_from
from stage_factory import factory

def make_pipeline(state):
    
    config = state.get_option('fastqs')
    
    pipeline = Pipeline(config.pipelineName())
    
    
    
    return pipeline