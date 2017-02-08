'''
Created on 21Nov.,2016

@author: thomas.e
'''

import subprocess
import resource
import os
import time
import argparse
import traceback
import tempfile
import shlex

from toil.job import Job
import logging

from wehi_pipeline.toil_support.jobStep import childrenOf
from wehi_pipeline.toil_support.jobStep import followersOf
from wehi_pipeline.toil_support.logger import StreamLogger


def tempDir(job):
    tmp = job.context.getTempDir()
    return tempfile.mkdtemp(prefix="t", dir=tmp)

def makedir(wdir):
    if os.path.isdir(wdir): 
        return
    
    if os.path.isfile(wdir):
        raise Exception(wdir + ' already exists as a file')
    
    os.makedirs(wdir)
 
def registerDrmaaBatchSystem():
    from toil.batchSystems.registry import addBatchSystemFactory
    from toil.batchSystems.options import addOptionsDefinition
    
    def drmaaBatchSystemFactory():
        from wehi_pipeline.batchSystems.drmaaBatchSystem import DrmaaBatchSystem
        return DrmaaBatchSystem
    
    addBatchSystemFactory('drmaa', drmaaBatchSystemFactory)
        
    def addOptions(addOptionFn):
        addOptionFn("--jobQueue", dest="jobQueue", default=None,
                help=("A job queue (used by the DRMAA batch system)"))
        
    addOptionsDefinition(addOptions)
    
def getOptions(desc):
    parser = argparse.ArgumentParser(desc, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--touchOnly', dest='touchOnly', action="store_true", default=False, help='Files are only touched, programs are not executed.')
    Job.Runner.addToilOptions(parser)

    options = parser.parse_args()
    options.disableCaching = True
    
    options.environment = ['PYTHONPATH', 'PATH']
    
    return options

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

def execute(job, cmds, pipeLineFiles, outfn=None, infn=None):
    
    touchOnly = job.context.touchOnly
    
    if pipeLineFiles is None:
        pipeLineFiles = []
        
    if type(pipeLineFiles) is not list:
        pipeLineFiles = [pipeLineFiles]
        
    if outfn is None or touchOnly:
        outfh = None
    else:
        outfh = open(outfn, 'w')
        
    if infn is None or touchOnly:
        infh = None
    else:
        infh = open(infn)
        
    if touchOnly:
        for f in pipeLineFiles:
            touch(f.path())
    else:
        try:
            osExecutor(cmds, outfh, infh)
        except Exception as exc:
                closeStream(outfh)            
                closeStream(infh)
                traceback.print_exc()
                raise exc
            
    for pf in pipeLineFiles:
        pf.commit()

def closeStream(stream):
    if stream is None:
        return
    
    try:
        stream.close()
    except:
        pass
    
                            
def osExecutor(cmds, outfh=None, infh=None):
    '''
    Execute a one or more commands in a pipe
    '''
        
    startR = resource.getrusage(resource.RUSAGE_CHILDREN)
    startT = time.time()
    
    if type(cmds) is not list:
        cmds = [cmds]
    ncmds = len(cmds)

    loggers = []
    processes = []
    inStream = infh

    for i in range(ncmds):
        cmd = cmds[i]
        
        logging.info('Executing: ' + cmd)
        
        last = i == (ncmds-1)
        
        if last:
            if outfh is None:
                outStream = subprocess.PIPE
                bufsize = 1
            else:
                outStream = outfh
                bufsize = 0
        else:
            bufsize = 0
            outStream = subprocess.PIPE
            
        cmdBits = shlex.split(cmd)
        cmdName = cmdBits[0]
        sp = subprocess.Popen(cmdBits, 
                          bufsize=bufsize,
                          stdout=outStream, 
                          stderr=subprocess.PIPE,
                          stdin=inStream
                          )
        sp.cmdName = cmdName
        
        processes.append(sp)
        logger = StreamLogger(logging.WARN if outfh is None else logging.INFO, cmdName)
        logger.redirect('child [%s] stderr' % cmdName, sp.stderr)
        loggers.append(logger)
        
        if last and outfh is None:
            logger = StreamLogger(logging.INFO, cmdName)
            logger.redirect('child [%s] stdout' % cmdName, sp.stdout)
            loggers.append(logger)
            
        inStream = sp.stdout

    (badRc, badCmd) = _pollChildren(processes)
    
    for logger in loggers:
        logger.shutdown()
        
    _reportUsage(startR, startT)
    
    if badRc:
        raise Exception(badCmd + '\ncompleted abnormally: rc=' + str(badRc))
 
def _pollChildren(processes):
    while len(processes) > 0:
        for process in processes[:]:
            time.sleep(10)
            rc = process.poll()
            if rc is not None:
                if rc:
                    # A process has failed
                    return (rc, process.cmdName)
                else:
                    # completed successfully
                    processes.remove(process)
    # all good    
    return (0, None)
        
def _reportUsage(startR, startT):
    msg = '%-25s (%-10s) = %s'
    
    endR = resource.getrusage(resource.RUSAGE_CHILDREN)
    endT = time.time()
    
    logging.info('-'*32 + ' Resource Usage ' + '-'*32)

    for name, desc in [
    ('ru_utime',   'User time'),
    ('ru_stime',   'System time'),
    ('ru_maxrss',  'Max. Resident Set Size'),
    ('ru_ixrss',   'Shared Memory Size'),
    ('ru_idrss',   'Unshared Memory Size'),
    ('ru_isrss',   'Stack Size'),
    ('ru_inblock', 'Block inputs'),
    ('ru_oublock', 'Block outputs'),
    ]:
        logging.info(msg % (desc, name, getattr(endR, name)-getattr(startR, name)))
        
    logging.info(msg % ('Elapsed time', '', endT-startT))
    logging.info('-'*80)
 
def launchNext(job, step):
    
    logging.info('Finished step:' + str(step))
    
    context = job.context 
    
    children = childrenOf(step, context.steps)
    if children is not None:
        for child in children:
            logging.info('Launching step: ' + str(child))
            nj = job.addChildJobFn(child)
            nj.context = context
            
    followers = followersOf(step, context.steps)
    if followers is not None:
        for follower in followers:
            nj = job.addFollowOnJobFn(follower)
            nj.context = context
            


