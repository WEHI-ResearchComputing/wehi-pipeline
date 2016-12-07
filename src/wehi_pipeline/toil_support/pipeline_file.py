'''
Created on 6Dec.,2016

@author: thomas.e
'''

import os
import shutil
import logging

class PipeLineObjectException(Exception):
    pass
    
class PipeLineDirectory(object):
    
    def __init__(self, job, fileKey=None, fileName=None, destPath=None):
        if fileName is None:
            raise PipeLineObjectException('fileName cannot be None for a directory')
        
        c = (1 if fileKey is None else 0) + (1 if destPath is None else 0)
        if c is not 1:
            raise PipeLineObjectException('Exactly one of fileKey or destPath must be specified')
        
        self.job = job
        self.fileKey = fileKey
        self.fileName = fileName
        self.destPath = destPath
        
    def create(self):
        self._path = self.job.fileStore.getLocalTempDir()
        
    def retrieve(self):
        raise PipeLineObjectException('Retrieving a PipeLineDirectory is not supported')
    
    def path(self):
        return self._path
    
    def commit(self):
        if self.fileKey is not None:
            path = os.path.join(self._path, self.fileName)
            _commitFile(self.job, path, self.fileKey)
        else:
            src  = os.path.join(self._path, self.fileName)
            dest = os.path.join(self.destPath, self.fileName)
            shutil.move(src, dest)

        
class PipeLineFile(object):
    '''
    classdocs
    '''

    def __init__(self, job, fileKey=None, fileName=None):
        '''
        Constructor
        '''
        
        if fileKey is None and fileName is None:
            raise PipeLineObjectException('A fileKey or a fileName must be supplied')
        
        self.job = job
        self.fileKey = fileKey
        self.fileName = fileName
        
    def create(self):
        if self.fileName is None:
            self._path =  self.job.fileStore.getLocalTempFile()
        else:
            outdir = self.job.fileStore.getLocalTempDir()
            self._path = os.path.join(outdir, self.fileName)
            
    def retrieve(self):
        files = self.job.context.files
        
        fid = files[self.fileKey]
        
        if self.fileName is None:
            self._path = self.job.fileStore.readGlobalFile(fid)
        else:
            outDir = self.job.fileStore.getLocalTempDir()
            self._path = os.path.join(outDir, self.fileName)
            self.job.fileStore.readGlobalFile(fid, self._path)
            
        return self._path
        
    def path(self):
        return self._path

    def commit(self):
        if self.fileKey is None:
            self._commitToDestination()
        else:
            self._commitToFileStore()
            
    def _commitToDestination(self):
        shutil.move(self._path, self.fileName)
        
    def _commitToFileStore(self):
        _commitFile(self.job, self._path, self.fileKey)
        pass
    
def _commitFile(job, path, fileKey):
    # Seems global name is obfuscated by design
    # Don't attempt to resolve it as that can potentially
    # trigger to create a local copy

    context = job.context

    if fileKey in context.files:
        raise PipeLineObjectException('The fileKey=' + fileKey + ' has already been used')
    
    fID = job.fileStore.writeGlobalFile(path, cleanup=False)
    context.files[fileKey] = fID
    
    statinfo = os.stat(path)
    sz = str(statinfo.st_size)
    inode = str(statinfo.st_ino)
    logging.info('Committing local description="%s", name=%s, size=%s, inode=%s toilId=%s' % (fileKey, path, sz, inode, str(fID)))
    
    return fID

    
class PipeLineBAMFile(object):
    '''
    classdocs
    '''

    def __init__(self, job, fileKey=None, fileName=None):
        '''
        Constructor
        '''
        
        if fileKey is None and fileName is None:
            raise PipeLineObjectException('A fileKey or a fileName must be supplied')
        
        self.job = job
        self.fileKey = fileKey
        self.fileName = fileName
        
    def create(self):
        filename = 'output' if self.fileName is None else self.fileName
        outDir = self.job.fileStore.getLocalTempDir()
        self.bamPath = os.path.join(outDir, filename + '.bam')
        self.baiPath = os.path.join(outDir, filename + '.bai')
        
        return self.bamPath
            
    def retrieve(self):
        files = self.job.context.files
        
        if self.fileKey not in files:
            raise PipeLineObjectException('fileKey=' + self.fileKey +' does not exist')
        
        filename = 'input' if self.fileName is None else self.fileName
        
        (BAMFID, BAIFID) = files[self.fileKey]
        
        outDir = self.job.fileStore.getLocalTempDir()
        self.baiPath = os.path.join(outDir, filename + '.bai')
        self.bamPath = os.path.join(outDir, filename + '.bam')
        
        self.job.fileStore.readGlobalFile(BAIFID, self.baiPath)
        self.job.fileStore.readGlobalFile(BAMFID, self.bamPath)
                    
        return self.bamPath
        
    def path(self):
        return self.bamPath

    def commit(self):
        if self.fileKey is None:
            self._commitToDestination()
        else:
            self._commitToFileStore()
            
    def _commitToDestination(self):
        shutil.move(self.baiPath, self.fileName)
        shutil.move(self.bamPath, self.fileName)
        
    def _commitToFileStore(self):
        files = self.job.context.files
        
        if self.fileKey in files:
            raise Exception('The fileKey=' + self.fileKey + ' has already been used')

        BAMFID = _commitFile(self.job, self.bamPath, self.fileKey + '-BAM')
        BAIFID = _commitFile(self.job, self.baiPath, self.fileKey + '-BAI')
        
        files[self.fileKey] = (BAMFID, BAIFID)


