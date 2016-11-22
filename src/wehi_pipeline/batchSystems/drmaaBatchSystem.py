# Copyright (c) 2016 Walter and Eliza Hall Institute for Medical Research
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
Created 21Nov.,2016

Evan Thomas
'''

from __future__ import absolute_import
import logging
import time
import os
from pipes import quote
from Queue import Queue, Empty
from threading import Thread
import drmaa

from toil.batchSystems.abstractBatchSystem import BatchSystemSupport

logger = logging.getLogger(__name__)

QUEUE_POLL_TIME = 1 # sec

class DrmaaWorker(Thread):
    def __init__(self, newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, config, boss):
        Thread.__init__(self)
        self.newJobsQueue = newJobsQueue
        self.updatedJobsQueue = updatedJobsQueue
        self.killQueue = killQueue
        self.killedJobsQueue = killedJobsQueue
        self.waitingJobs = list()
        self.runningJobs = set()
        self.boss = boss
        self.allocatedCpus = dict()
        self.drmaaJobIDs = dict()
        self.workDir = config.workDir
        
        if config.environment:
            self.environment = {}
            for k, v in config.environment.iteritems():
                quoted_value = quote(os.environ[k] if v is None else v)
                self.environment[k] = quoted_value
            if len(self.environment) == 0:
                self.environment = None
        else:
            self.environment = None
                
        self.jobQueue = config.jobQueue        
        s=drmaa.Session()
        s.initialize()
        self.drmaaSession = s

    def parse_elapsed(self, elapsed):
        # drmaa returns elapsed time in days-hours:minutes:seconds format
        # Sometimes it will only return minutes:seconds, so days may be omitted
        # For ease of calculating, we'll make sure all the delimeters are ':'
        # Then reverse the list so that we're always counting up from seconds -> minutes -> hours -> days
        total_seconds = 0
        try:
            elapsed = elapsed.replace('-', ':').split(':')
            elapsed.reverse()
            seconds_per_unit = [1, 60, 3600, 86400]
            for index, multiplier in enumerate(seconds_per_unit):
                if index < len(elapsed):
                    total_seconds += multiplier * int(elapsed[index])
        except ValueError:
            pass  # drmaa may return INVALID instead of a time
        return total_seconds

    def getdrmaaID(self, jobID):
        if not jobID in self.drmaaJobIDs:
            RuntimeError("Unknown jobID, could not be converted")

        job = self.drmaaJobIDs[jobID]
        return str(job)

    def forgetJob(self, jobID):
        self.runningJobs.remove(jobID)
        del self.allocatedCpus[jobID]
        del self.drmaaJobIDs[jobID]

    def killJobs(self):
        # Load hit list:
        killList = list()
        while True:
            try:
                jobId = self.killQueue.get(block=False)
            except Empty:
                break
            else:
                killList.append(jobId)

        if not killList:
            return False

        # Do the dirty job
        for jobID in list(killList):
            if jobID in self.runningJobs:
                logger.debug('Killing job: %s', jobID)
                self.drmaaSession.control(self.getdrmaaID(jobID), drmaa.JobControlAction.TERMINATE)
            else:
                if jobID in self.waitingJobs:
                    self.waitingJobs.remove(jobID)
                self.killedJobsQueue.put(jobID)
                killList.remove(jobID)

        # Wait to confirm the kill
        while killList:
            for jobID in list(killList):
                if self.getJobExitCode(self.drmaaJobIDs[jobID]) is not None:
                    logger.debug('Adding jobID %s to killedJobsQueue', jobID)
                    self.killedJobsQueue.put(jobID)
                    killList.remove(jobID)
                    self.forgetJob(jobID)
            if len(killList) > 0:
                logger.warn("Some jobs weren't killed, trying again in %is.", QUEUE_POLL_TIME)
                time.sleep(QUEUE_POLL_TIME)

        return True

    def createJobs(self, newJob):
        activity = False
        # Load new job id if present:
        if newJob is not None:
            self.waitingJobs.append(newJob)
        # Launch jobs as necessary:
        while (len(self.waitingJobs) > 0
               and sum(self.allocatedCpus.values()) < int(self.boss.maxCores)):
            activity = True
            jobID, cpu, memory, command = self.waitingJobs.pop(0)
            drmaaJobID = self.submit(jobID, cpu, memory, command)
            self.drmaaJobIDs[jobID] = drmaaJobID
            self.runningJobs.add(jobID)
            self.allocatedCpus[jobID] = cpu
        return activity

    def checkOnJobs(self):
        activity = False
        logger.debug('List of running jobs: %r', self.runningJobs)
        for jobID in list(self.runningJobs):
            logger.debug("Checking status of internal job id %d", jobID)
            status = self.getJobExitCode(self.drmaaJobIDs[jobID])
            if status is not None:
                activity = True
                self.updatedJobsQueue.put((jobID, status))
                self.forgetJob(jobID)
        return activity

    def run(self):
        while True:
            activity = False
            newJob = None
            if not self.newJobsQueue.empty():
                activity = True
                newJob = self.newJobsQueue.get()
                if newJob is None:
                    logger.debug('Received queue sentinel.')
                    break
            activity |= self.killJobs()
            activity |= self.createJobs(newJob)
            activity |= self.checkOnJobs()
            if not activity:
                logger.debug('No activity, sleeping for %is', QUEUE_POLL_TIME)
                time.sleep(QUEUE_POLL_TIME)

    def submit(self, jobID, cpu, memory, command):
        session = self.drmaaSession
        
        jt = session.createJobTemplate()
        jt.workingDirectory = self.workDir
        jt.remoteCommand = command
        jt.outputPath = self.workDir

        if self.environment is not None:
            jt.jobEnvironment = self.environment
            
        if session.drmsInfo == u'Torque':
            try:
                jt.nativeSpecification = '-q ' + self.jobQueue
            except NameError:
                # No job queue specified
                pass
        
        return session.runJob(jt)
        
    def getJobExitCode(self, drmaaJobID):
        logger.debug("Getting exit code for drmaa job %s", str(drmaaJobID))
        try:
            # Wait second, then assume job has not completed
            info=self.drmaaSession.wait(drmaaJobID, QUEUE_POLL_TIME)
            
            if info is None:
                return None
            
            if not info.hasExited:
                return None
            
            if info.hasSignal:
                return info.terminatedSignal
            
            return info.exitStatus
        
        except drmaa.errors.ExitTimeoutException:
            return None

def __truncate__(f):
    bits = f.split(':')
    if len(bits) == 1:
        fn = bits[0]
    else:
        protocol = bits[0]
        if not protocol == 'file':
            raise RuntimeError('The drmaa results file protocol is not support: ' + f)
        fn = bits[1]
    h = open(fn, 'w')
    h.close()
        

class DrmaaBatchSystem(BatchSystemSupport):
    '''
    Interface for DRMAA supported batch systems
    '''
    
    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsHotDeployment(cls):
        return False

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(DrmaaBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

        self.drmaaResultsFile = self._getResultsFileName(config.jobStore)
        
        # Reset the job queue and results (initially, we do this again once we've killed the jobs)
        # We lose any previous state in this file, and ensure the files existence
        __truncate__(self.drmaaResultsFile)
        
        self.currentJobs = set()
        self.maxCPU, self.maxMEM = self.obtainSystemConstants()
        self.nextJobID = 0
        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.killQueue = Queue()
        self.killedJobsQueue = Queue()
        self.worker = DrmaaWorker(self.newJobsQueue, 
                                  self.updatedJobsQueue, 
                                  self.killQueue,
                                  self.killedJobsQueue,
                                  config,
                                  self)
        
        self.worker.start()
        
    def __des__(self):
        try:
            # Closes the file handle associated with the results file.
            self.drmaaResultsFileHandle.close()
        except:
            pass

    def issueBatchJob(self, command, memory, cores, disk, preemptable):
        self.checkResourceRequest(memory, cores, disk)
        jobID = self.nextJobID
        self.nextJobID += 1
        self.currentJobs.add(jobID)
        self.newJobsQueue.put((jobID, cores, memory, command))
        logger.debug("Issued the job command: %s with job id: %s ", command, str(jobID))
        return jobID
    
    
    def killBatchJobs(self, jobIDs):
        """
        Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        jobIDs = set(jobIDs)
        logger.debug('Jobs to be killed: %r', jobIDs)
        for jobID in jobIDs:
            self.killQueue.put(jobID)
        while jobIDs:
            killedJobId = self.killedJobsQueue.get()
            if killedJobId is None:
                break
            jobIDs.remove(killedJobId)
            if killedJobId in self.currentJobs:
                self.currentJobs.remove(killedJobId)
            if jobIDs:
                logger.debug('Some kills (%s) still pending, sleeping %is', len(jobIDs),
                             QUEUE_POLL_TIME)
                time.sleep(QUEUE_POLL_TIME)

    
    def getIssuedBatchJobIDs(self):
        """
        Gets the list of jobs issued to drmaa.
        """
        return list(self.currentJobs)
    
    def getRunningBatchJobIDs(self):
        return self.worker.getRunningJobIDs()

    def getUpdatedBatchJob(self, maxWait):
        try:
            item = self.updatedJobsQueue.get(timeout=maxWait)
        except Empty:
            return None
        logger.debug('UpdatedJobsQueue Item: %s', item)
        jobID, retcode = item
        self.currentJobs.remove(jobID)
        return jobID, retcode, None

    def shutdown(self):
        """
        Signals worker to shutdown (via sentinel) then cleanly joins the thread
        """
        newJobsQueue = self.newJobsQueue
        self.newJobsQueue = None

        newJobsQueue.put(None)
        self.worker.join()

    def getWaitDuration(self):
        return 1.0

    @classmethod
    def getRescueBatchJobFrequency(cls):
        return 30 * 60 # Half an hour
    
    
    @staticmethod
    def obtainSystemConstants():
        '''
        Just make something up ...
        '''
        return 51, 511*1024

    @staticmethod
    def setOptions(setOption):
        setOption("jobQueue")

        
        