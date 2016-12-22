'''
Created on 6Dec.,2016

@author: thomas.e
'''

import os
import shutil
import logging

class PipeLineFileObjectException(Exception):
    pass
    
class PipeLineDirectory(object):
    
    def __init__(self, job, fileKey=None, fileName=None, destPath=None):
        
        c = (1 if fileKey is None else 0) + (1 if destPath is None else 0)
        if c is not 1:
            raise PipeLineFileObjectException('Exactly one of fileKey or destPath must be specified')
        
        self.job = job
        self.fileKey = fileKey
        self.fileName = fileName
        self.destPath = destPath
        
    def create(self):
        self._path = self.job.fileStore.getLocalTempDir()
        
    def retrieve(self):
        raise PipeLineFileObjectException('Retrieving a PipeLineDirectory is not supported')
    
    def path(self):
        return self._path
    
    def commit(self):
        if self.fileKey is not None:
            path = os.path.join(self._path, self.fileName)
            _commitFile(self.job, path, self.fileKey)
        else:
            fileName = self.fileName
            
            if fileName is None:
                srcFiles = os.listdir(self._path)
            else:
                srcFiles  = [fileName]
                
            for f in srcFiles:
                tgt = os.path.join(self.destPath, f)
                src = os.path.join(self._path, f)
                if os.path.isfile(tgt):
                    os.remove(tgt)
                if os.path.isdir(tgt):
                    os.rmdir(tgt)
                shutil.move(src, tgt)

        
class PipeLineFile(object):

    def __init__(self, job, fileKey=None, fileName=None, destDir=None, shareDirectoryWith=None):
        '''
        Constructor
        '''
        
        if fileKey is None and destDir is None and fileName is None:
            raise PipeLineFileObjectException('A fileKey, destDir for fileName must be supplied')

        if shareDirectoryWith is not None and fileName is None:
            raise PipeLineFileObjectException('Files that share directories must be named. Please provide a fileName.')
        
        self.job = job
        self.fileKey = fileKey
        self.fileName = fileName
        self.destDir = destDir
        self.shareWith = shareDirectoryWith
        
    def create(self):
        if self.fileName is None:
            self._path =  self.job.fileStore.getLocalTempFile()
        else:
            if self.shareWith is None:
                outdir = self.job.fileStore.getLocalTempDir()
            else:
                outdir = self.shareWith.dir()
            self._path = os.path.join(outdir, self.fileName)
            
        logging.info('File created: ' + self.path())
            
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
    
    def dir(self):
        if os.path.isdir(self._path):
            return self.path()
        else:
            return os.path.dirname(self._path)

    def commit(self):
        if self.fileKey is None:
            self._commitToDestination()
        else:
            self._commitToFileStore()
            
    def _commitToDestination(self):
        fileName = self.fileName
        tgt = None
        if fileName is not None:
            tgt = os.path.join(self.destDir, self.fileName)
            if os.path.isfile(tgt):
                os.remove(tgt)
            if os.path.isdir(tgt):
                raise Exception('fileName=' + fileName + ' already exists as a directory')
        
        shutil.move(self._path, self.destDir)
        
        if tgt is None:
            fn = os.path.basename(self._path)
            tgt = os.path.join(self._path, fn)
            
        _logCommittedFileDetails(tgt, None, None)
        
    def _commitToFileStore(self):
        _commitFile(self.job, self._path, self.fileKey)
        pass
    
def _logCommittedFileDetails(path, fileKey, fID):
    statinfo = os.stat(path)
    sz = str(statinfo.st_size)
    inode = str(statinfo.st_ino)
    if fileKey is None:
        logging.info('Committing final file, name=%s, size=%s, inode=%s' % (path, sz, inode))
    else:
        logging.info('Committing local description="%s", name=%s, size=%s, inode=%s toilId=%s' % (fileKey, path, sz, inode, str(fID)))
    
def _commitFile(job, path, fileKey):
    # Seems global name is obfuscated by design
    # Don't attempt to resolve it as that can potentially
    # trigger to create a local copy

    context = job.context

    if fileKey in context.files:
        raise PipeLineFileObjectException('The fileKey=' + fileKey + ' has already been used')
    
    fID = job.fileStore.writeGlobalFile(path, cleanup=False)
    context.files[fileKey] = fID
    
    _logCommittedFileDetails(path, fileKey, fID)
    
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
            raise PipeLineFileObjectException('A fileKey or a fileName must be supplied')
        
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
            raise PipeLineFileObjectException('fileKey=' + self.fileKey +' does not exist')
        
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


