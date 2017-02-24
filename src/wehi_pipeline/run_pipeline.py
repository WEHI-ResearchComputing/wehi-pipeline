'''
Created on 6Feb.,2017

@author: thomas.e
'''
from wehi_pipeline.steps.jobStep import wehiWrapper

import argparse
import os
import sys
import dill

from toil.common import Toil
from toil.job import Job
import logging

from wehi_pipeline.config.config import Config
from wehi_pipeline.toil_support.context import WorkflowContext
from wehi_pipeline.toil_support.utils import registerDrmaaBatchSystem

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
        print('No input files')
        return None
 
    contexts = []
    for fq in fqs:
        cxt = WorkflowContext(fq.forward(), fq.backward(), fq.sample(), TMPBASE, config.symbols())
        cxt = dill.dumps(cxt)
        contexts.append(cxt)

    firstStep = config.steps()[0]
    
    mj = Job()
    for context in contexts:
        mj.addChildJobFn(wehiWrapper, step=firstStep, context=context)
        
    return mj

def main():
    registerDrmaaBatchSystem()
    
    options = getOptions()
    
    configFile = options.config
    config = Config(configFile)
    
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