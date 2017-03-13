'''
Created on 15Feb.,2017

@author: thomas.e
'''

import os

from wehi_pipeline.steps.jobStep import ConfigJobStep
from wehi_pipeline.toil_support.utils import execute
from wehi_pipeline.toil_support.shellModules import javaHelper
from wehi_pipeline.config.symbols import resolveSymbols, findOutputFile
from wehi_pipeline.config import ConfigException

TRIMMOMATIC = 'trimmomatic'
ADAPTORS = 'ILLUMINACLIP:/stornext/System/data/apps/trimmomatic/trimmomatic-0.36/adapters/TruSeq3-PE.fa:1:30:20:4:true'
NP_THREADS = 8


class Trim(ConfigJobStep):

    def __init__(self, config):
            
        if 'name' not in config:
            config['name'] = 'trim'
            
        config['modules'] = [TRIMMOMATIC]
        
        config['outputs'] = [
            {
            'name' :  'trimmed-forward',
            'destination' : '$temp-dir',
            'filename' : 'trimmed_1P.fastq.gz' 
            },
            {
            'name' :  'trimmed-backward',
            'destination' : '$temp-dir',
            'filename' : 'trimmed_2P.fastq.gz',
            'share-directory-with' : 'trimmed-forward'
            }
                   ]
        
        super(Trim, self).__init__(config)
        
        if 'trimmer' in config:
            self._trimmer = config['trimmer']
            if self._trimmer != TRIMMOMATIC:
                raise ConfigException('Only %s is supporter at the moment' % TRIMMOMATIC)
            
    def numRecommendedThreads(self):
        return NP_THREADS
    
    def recommendedMemory(self):
        return 60
    
    def function(self):
        def f(job, context):
            
            numThreads = str(self.numRecommendedThreads())
            ram = str(self.recommendedMemory())
            
            jar = javaHelper(self.modules(), TRIMMOMATIC)
            if jar is None:
                raise Exception('Could not find ' + TRIMMOMATIC + ' jar')
                        
            cmd = 'java -Djava.io.tmpdir=$temp-dir ' + '-Xmx' + ram + 'g -jar ' + jar + ' PE -threads ' + numThreads + ' $forward  $backward' 
            (cmd, outputFiles) = resolveSymbols(job, context, cmd, self.symbols())
            
            anOutput = findOutputFile(context, 'trimmed-backward')
            baseOut = os.path.join(os.path.dirname(anOutput.path()), 'trimmed.fastq.gz')
            
            adaptors = self._getAdaptors(jar)
            
            cmd = cmd[0] + ' -baseout ' + baseOut + ' ' + adaptors
            
            execute(context, cmd, outputFiles, modules=['trimmomatic'])
            
            return context
                
        return f

    def _getAdaptors(self, jar):
        # Apaptors could be worked out using the location of the trimmer software_version
        return ADAPTORS
    