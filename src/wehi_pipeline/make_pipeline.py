'''
Created on 14Oct.,2016

@author: thomas.e
'''
from ruffus import Pipeline, suffix, formatter, add_inputs, output_from
from wehi_pipeline.stage_factory import registry

def make_pipeline(state, extra_factories=None):
    
    config = state.config
    if config is None:
        raise Exception('The state object has no configuration.')

    pipeline = Pipeline(config.pipelineName())
    
    # Get a list of paths to all the FASTQ files
    fastq_files = config.get_option('fastqs')
    if fastq_files is not None:
        # The original FASTQ files
        # This is a dummy stage. It is useful because it makes a node in the
        # pipeline graph, and gives the pipeline an obvious starting point.
        pipeline.originate(
            task_func=lambda x: None,
            name='original_fastqs',
            output=fastq_files)

    human_reference_genome_file = state.config.get_option('human_reference_genome')
    if human_reference_genome_file is not None:
        # The human reference genome in FASTA format
        pipeline.originate(
            task_func=lambda x: None,
            name='human_reference_genome',
            output=human_reference_genome_file)
        
    last_output = fastq_files if fastq_files is not None else human_reference_genome_file

    stages = config.stages()
    if stages is None or len(stages) == 0:
        raise Exception('No stages - nothing to do')
    
    for stage in stages:
        t = stage.type
        try:
            sf = registry[t]
        except KeyError:
            raise Exception('The stage type ' + t + ' is not known')
        
        last_output = sf.insert_into_pipeline(config, pipeline, stage, last_output)
        
    return pipeline