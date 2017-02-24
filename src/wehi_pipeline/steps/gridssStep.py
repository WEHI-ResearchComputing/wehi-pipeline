'''
Created on 15Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import ConfigJobStep
from wehi_pipeline.config.symbols import resolveSymbols
from wehi_pipeline.toil_support.utils import execute
from wehi_pipeline.config import ConfigException

GRIDSS = '/home/thomas.e/mnWork/gridss-1.2.4-jar-with-dependencies.jar'
NP_THREADS = 8

class Gridss(ConfigJobStep):


    def __init__(self, config):
                    
        if 'name' not in config:
            config['name'] = 'trim'
            
        config['modules'] = [GRIDSS]
        
        super(Gridss, self).__init__(config)
        
        vcfOutputFound = False
        assemblyOutputFound = False
        
        for o in self.outputs():
            if o.name() == 'vcf':
                vcfOutputFound = True
            if o.name() == 'assembly':
                assemblyOutputFound = True

        if not vcfOutputFound or not assemblyOutputFound:
            raise ConfigException('A vcf and assembly output must be provided.')

    def function(self):
        def f(job, context):
    
            ram = 8 + 2 * NP_THREADS
            ram = '-Xmx' + str(ram) + 'g'

            cmd = 'java -ea ' + ram + \
                ' -Dsamjdk.create_index=true' + \
                ' -Dsamjdk.use_async_io_read_samtools=true' + \
                ' -Dsamjdk.use_async_io_write_samtools=true' + \
                ' -Dsamjdk.use_async_io_write_tribble=true' + \
                ' -Dsamjdk.compression_level=1' + \
                ' -cp ' + GRIDSS + ' gridss.CallVariants' + \
                ' TMP_DIR=$temp-dir' + \
                ' WORKING_DIR=$temp-dir' + \
                ' REFERENCE_SEQUENCE=$hg38' + \
                ' ASSEMBLY=$assembly' + \
                ' INPUT=$sorted IC=1' + \
                ' OUTPUT=$vcf' + \
                ' THREADS=' + str(NP_THREADS)
            
            (cmd, outputFiles) = resolveSymbols(job, context, cmd, self.symbols())
            
            execute(context, cmd, outputFiles, modules=self.modules())
            
            return context
    
        return f
