'''
Created on 6Feb.,2017

@author: thomas.e
'''

import argparse
import sys

from toil.common import Toil
from toil.job import Job
import logging

from wehi_pipeline.config.config import Config

def getOptions(name):
    parser = argparse.ArgumentParser(name, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--touchOnly', dest='touchOnly', action="store_true", default=False, help='Files are only touched, programs are not executed.')
    Job.Runner.addToilOptions(parser)

    options = parser.parse_args(args=['wehi-pipeline'])
    options.disableCaching = True
        
def makeLaunchJob(touchOnly):
    contexts = []#getContextList(touchOnly)
    
    if contexts is None:
        return None

    mj = Job()
    startFunction = None #steps[0].function
    for context in contexts:
        nj = mj.addChildJobFn(startFunction)
        nj.context = context
        
    return mj

def main():
    configFile = sys.argv[1]
    
    config = Config(configFile)
    
    if not config.isValid():
        print('Configuration is not valid:')
        print config.validationErrors()[0]
        sys.exit(1)
        
    options = getOptions(config.name())
    
    mj = makeLaunchJob(options.touchOnly)
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