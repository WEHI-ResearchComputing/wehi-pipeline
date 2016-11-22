'''
Created on 25Oct.,2016

@author: thomas.e
'''

from ruffus import formatter, add_inputs, output_from

def factory(config, pipeline, stage, last_output):
    
    pipeline.transform(
        task_function=trim,
        name=stage.name,
        input=output_from(last_output),
        # Match the R1 (read 1) FASTQ file and grab the path and sample name. 
        # This will be the first input to the stage.
        # We assume the sample name may consist of only alphanumeric
        # characters.
        filter=formatter('.+/(?P<sample>[_a-zA-Z0-9]+)_R1.fastq.gz'),
        # Add one more inputs to the stage:
        #    1. The corresponding R2 FASTQ file
        add_inputs=add_inputs('{path[0]}/{sample[0]}_R2.fastq.gz'),
        # Add an "extra" argument to the state (beyond the inputs and outputs)
        # which is the sample name. This is needed within the stage for finding out
        # sample specific configuration options
        extras=['{sample[0]}'],
        # The output file name is the sample name with a .bam extension.
        output='{path[0]}/{sample[0]}.bam')
    
def trim(inputs, bam_out, sample_id):
    fastq_read1_in, fastq_read2_in = inputs
    
