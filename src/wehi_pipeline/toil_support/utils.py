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
        
def osExecutor(cmd, outfn=None):
    if type(cmd) is not list:
        cmd = cmd.split()
        
    logging.info('Executing: ' + ' '.join(cmd))
    
    if outfn is None:
        sp = subprocess.Popen(cmd, 
                          bufsize=1,
                          stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE
                          )
        stdoutLogger = StreamLogger(logging.INFO)
        stdoutLogger.redirect('child stdout', sp.stdout)
    else:
        stdoutLogger = None
        outfn = open(outfn, 'wb')
        sp = subprocess.Popen(cmd, 
                          stdout=outfn, 
                          stderr=subprocess.PIPE
                          )
    
    stderrLogger = StreamLogger(logging.WARN if outfn is None else logging.INFO)
    stderrLogger.redirect('child stderr', sp.stderr)

    sp.wait()
    rc = sp.returncode
    
    if stdoutLogger is not None:
        stdoutLogger.shutdown()
    stderrLogger.shutdown()

    if rc:
        raise Exception(' '.join(cmd) + '\ncompleted abnormally: rc=' + str(rc))
 
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
            
