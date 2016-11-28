'''
Created on 21Nov.,2016

@author: thomas.e
'''

import subprocess
import os
import argparse

from toil.job import Job
import logging

from wehi_pipeline.toil_support.jobStep import childrenOf
from wehi_pipeline.toil_support.jobStep import followersOf
from wehi_pipeline.toil_support.logger import StreamLogger

def makedir(wdir):
    if os.path.isdir(wdir): 
        return
    
    if os.path.isfile(wdir):
        raise Exception(wdir + ' already exists as a file')
    
    os.makedirs(wdir)
 
def commitFile(job, fn, desc):
    # Seems global name is obfuscated by design
    # Don't attempt to resolve it as that can potentially
    # trigger to create a local copy
    fID = job.fileStore.writeGlobalFile(fn, cleanup=False)
    statinfo = os.stat(fn)
    sz = str(statinfo.st_size)
    logging.info('Committing local description="%s", name=%s, size=%s, id=%s' % (desc, fn, sz, str(fID)))
    
    return fID

def registerDrmaaBatchSystem():
    from toil.batchSystems.registry import addBatchSystemFactory
    from toil.batchSystems.options import addOptionsDefinition
    
    def drmaaBatchSystemFactory():
        from wehi_pipeline.batchSystems.drmaaBatchSystem import DrmaaBatchSystem
        return DrmaaBatchSystem
    
    addBatchSystemFactory('drmaa', drmaaBatchSystemFactory)
        
    def addOptions(addOptionFn):
        addOptionFn("--jobQueue", dest="jobQueue", default=None,
                help=("A job queue (used by the DRMAA batch system"))
        
    addOptionsDefinition(addOptions)
    
def getOptions(desc):
    parser = argparse.ArgumentParser(desc, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--touchOnly', dest='touchOnly', action="store_true", default=False, help='Files are only touched, programs are not executed.')
    Job.Runner.addToilOptions(parser)

    options = parser.parse_args()
    options.disableCaching = True
    options.environment = ['PYTHONPATH', 'TMP']
    
    return options

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)
        
def osExecutor(cmds, outfn=None, infn=None):
    '''
    Execute a one or more commands in a pipe
    '''
        
    if type(cmds) is not list:
        cmds = [cmds]

    loggers = []
    inStream = infn

    for cmd in cmds:
        logging.info('Executing: ' + cmd)
        
        last = cmd == cmd[-1]
        
        if last and outfn is None:
            bufsize = 1
        else:
            bufsize = 0
            outStream = subprocess.PIPE
            
        cmdBits = cmd.split()
        cmdName = cmdBits[0]
        sp = subprocess.Popen(cmdBits, 
                          bufsize=bufsize,
                          stdout=outStream, 
                          stderr=subprocess.PIPE,
                          stdin=inStream
                          )
        
        logger = StreamLogger(logging.WARN if outfn is None else logging.INFO, cmdName)
        logger.redirect('child [%s] stderr' % cmdName, sp.stderr)
        loggers.append(logger)
        
        if last and outfn is None:
            logger = StreamLogger(logging.INFO, cmdName)
            logger.redirect('child [%s] stdout' % cmdName, sp.stdout)
            loggers.append()
            
        inStream = sp.stdout
        
    sp.wait()
    rc = sp.returncode
    
    for logger in loggers:
        logger.shutdown()

    if rc:
        raise Exception(cmd + '\ncompleted abnormally: rc=' + str(rc))
 
def launchNext(job, step, context):
    
    logging.info('Finished step:' + str(step))
    
    children = childrenOf(step, context.steps)
    if children is not None:
        for child in children:
            logging.info('Launching step: ' + str(child))
            job.addChildJobFn(child, context)
            
    followers = followersOf(step, context.steps)
    if followers is not None:
        for follower in followers:
            job.addFollowOnJobFn(follower, context)
            
