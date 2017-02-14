'''
Created on 7Feb.,2017

@author: thomas.e
'''

import re
import subprocess

from wehi_pipeline.toil_support.pipeline_file import PipeLineBAMFile, PipeLineFile
from wehi_pipeline.toil_support.utils import asList

class AbstractSymbol(object):
    
    def __init__(self, name):
        self._name = name
        
    def name(self):
        return self._name
    
    def value(self, job):
        raise NotImplemented
    
    def tokens(self):
        raise NotImplemented

    def updateTokens(self, tokens):
        raise NotImplemented
    
class ValueSymbol(object):
    
    def __init__(self, name, value):  
        self._name = name
        self._value = value
    
    def value(self):
        return self._value
    
    def tokens(self):
        return _getTokens(self._value)
    
    def updateTokens(self, tokens):
        self._value = _replaceTokens(self._value, tokens)
    
class BuiltInSymbol(AbstractSymbol):
    
    def __init__(self, name):
        super(BuiltInSymbol, self).__init__(name)
        
        self._value = None
        self._resolved = False
        
    def resolve(self, job):
        if self._resolved:
            return
        self._resolved = True
        
        context = job.context
        
        if self._name == 'temp-dir':
            self._value = context.getTempDir()
            return
        
        if self._name == 'sample':
            self._value = context.identifier
            return
        
        if self._name == 'forward':
            self._value = context.forward
        
        if self._name == 'backward':
            self._value = context.backward

    def value(self, job):
        if not self._resolved:
            self.resolve(job)
        return self._value
        
    def tokens(self):
        pass

    def updateTokens(self, tokens):
        pass

_builtin_symbol_map = {
    'sample'   : BuiltInSymbol('sample'),
    'forward'  : BuiltInSymbol('forward'),
    'backward' : BuiltInSymbol('backward'),
    'temp-dir' : BuiltInSymbol('temp-dir'),
    }
    
class DestinationSymbol(ValueSymbol):
    
    def __init__(self, name, path):
        super(DestinationSymbol, self).__init__(name, path)
        self._path = path
        
    def value(self):
        return self._path
    
    def tokens(self):
        return _getTokens(self._path)
    
    def updateTokens(self, tokens):
        self._path = _replaceTokens(self._path, tokens)
    
class ReferenceSymbol(ValueSymbol):
    
    def __init__(self, name, path):
        super(ReferenceSymbol, self).__init__(name, path)
        self._path = path
    
    def value(self, job):
        return self._path
    
    def tokens(self):
        return _getTokens(self._path)
    
    def updateTokens(self, tokens):
        self._path = _replaceTokens(self._path, tokens)

class Output(AbstractSymbol):
    
    def __init__(self, config):
        super(Output, self).__init__(config['name'])

        self._type = config['type'] if 'type' in config else 'regular'
        self._filename    = config['filename'] if 'filename' in config else None
        self._destination = config['destination'] if 'destination' in config else None
        self._fileKey = self._name if self._destination is None else None
        
        self._created = False
        self._resolved = False
        
    def resolve(self, job):
        if self._resolved:
            return
        self._resolved = True
        
        if self._type == 'regular':
            self._plFile = PipeLineFile(job, fileKey=self._fileKey, destDir=self._destination, fileName=self._filename)
        else:
            self._plFile = PipeLineBAMFile(job, fileKey=self._fileKey, destDir=self._destination, fileName=self._filename)
            
        if self._created:
            self._plFile.retrieve()
        else:
            self._plFile.create()
            self._created = True

    def value(self, job):
        if not self._resolved:
            self.resolve(job)
        return self._plFile.path()
    
    def tokens(self):
        return _getTokens(self._destination) + _getTokens(self._filename)
    
    def updateTokens(self, tokens):
        self._destination = _replaceTokens(self._destination, tokens)
        self._filename = _replaceTokens(self._filename, tokens)
    
    def pipeLineFile(self):
        return self._plFile
    
class PreCommand(AbstractSymbol):
    
    def __init__(self, config):
        super(PreCommand, self).__init__(config['variable'])
        self._command = config['command']
        self._resolved = False

    def resolve(self, job):
        if self._resolved:
            return
        pu = subprocess.check_output(self._command, shell=True) 
        self._value = pu.strip()
        self._resolved = True
       
    def value(self, job):
        if not self._resolved:
            self.resolve(job)
        return self._value
    
    def tokens(self):
        return _getTokens(self._command)
    
    def updateTokens(self, tokens):
        self._command = _replaceTokens(self._command, tokens)
        
def findSymbol(token, tables):
    for table in tables:
        if token in table:
            return table[token]
    return None

def evaluate(job, s, tables):
    tables.insert(0, builtInSymbols())
    
    tokens = dict()
    for table in tables:
        for symbol in table:
            symbol.updateTokens(tokens)
            tokens[symbol.name()] = symbol.value(job)
            
    return _replaceTokens(s, tokens)

def builtInSymbols():
    return _builtin_symbol_map.copy().values()

def _getTokens(s):
    tks = re.findall('\$[0-9a-zA-Z]*', s)
    tokenSet = set()
    for t in tks:
        tokenSet.add(t[1:])
    return tokenSet

def _replaceTokens(cmd, tokens):
    if cmd is None:
        return None
    
    for (token, value) in tokens.iteritems():
        if value is not None:
            cmd = re.sub('\$'+token, value, cmd)

    return cmd


def _isEmpty(thing):
    if thing is None:
        return True
    if type(thing) is list:
        return len(thing) == 0
    return False

def resolveSymbols(job, commands, config, stepConfig):
    
    if _isEmpty(commands):
        return (None, None)
    
    commands = asList(commands)
    
    processedCommands = []
    outFiles = []
    for cmd in commands:
        (processedCommand, fs) = resolveString(job, cmd, config, stepConfig)
        processedCommands.append(processedCommand)
        outFiles = outFiles + fs
        
    return (processedCommand, outFiles)

def resolveString(job, cmd, config, stepConfig):

    knownFiles = job.context.knownFiles
    cmd = evaluate(job, cmd, [config, stepConfig.symbols(), knownFiles])

    outputFiles = []    
    for symbol in stepConfig.symbols():
        if type(symbol) is Output:
            outputFiles.append(symbol.pipeLineFile())
            knownFiles.append(symbol)
            
            
    return (cmd, outputFiles)
