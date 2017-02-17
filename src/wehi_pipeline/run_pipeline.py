'''
Created on 6Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import wehiWrapper

HOST = '10.1.17.158'
import pydevd

import argparse
import os
import sys

from toil.common import Toil
from toil.job import Job
import logging

from wehi_pipeline.config.config import Config
from wehi_pipeline.toil_support.context import WorkflowContext

TMPBASE = os.path.join(os.getenv('HOME'), 'tmp')

def getOptions():
    parser = argparse.ArgumentParser('python run_pipeline', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--touchOnly', dest='touchOnly', action="store_true", default=False, help='Files are only touched, programs are not executed.')
    parser.add_argument('--config', dest='config', type=str, help='The pipeline definition in YAML format')
    Job.Runner.addToilOptions(parser)

    args = sys.argv[1:] + ['wehi-pipeline']
    options = parser.parse_args(args=args)
    options.disableCaching = True
    
    return options
        
def makeLaunchJob(config):
        
    fqs = config.fastqs()
    if len(fqs) == 0:
        logging.info('No input files')
        return None
 
    contexts = []
    for fq in fqs:
        contexts.append(WorkflowContext(fq.forward(), fq.backward(), fq.sample(), TMPBASE, None))

    mj = Job()
    firstStep = config.steps()[0]
    
    for context in contexts:
        nj = mj.addChildJobFn(wehiWrapper, step=firstStep)
        nj.context = context
        
    return mj

def main():
    pydevd.settrace(HOST, stdoutToServer=True, stderrToServer=True, suspend=False)
        
    options = getOptions()
    
    configFile = options.config
    config = Config(configFile)
    
    if not config.isValid():
        print('Configuration is not valid:')
        print config.validationErrors()[0]
        sys.exit(1)
        

    mj = makeLaunchJob(config)
    if mj is None:
        return
    
    with Toil(options) as t:
        if options.restart:
            logging.info('Restarting old run.')
            t.restart()
        else:
            logging.info('Starting new run.')
            t.start(mj)     
    
if __name__ == '__main__':
    main()