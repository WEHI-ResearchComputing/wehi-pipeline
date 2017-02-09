'''
Created on 6Feb.,2017

@author: thomas.e
'''
import re
import subprocess

from wehi_pipeline.config import ConfigException
from wehi_pipeline.symbols import findSymbol
from wehi_pipeline.toil_support.utils import execute

def stepFactory(stepConfig):
    stepType = stepConfig.keys()[0]
    stepValue = stepConfig[stepType]
     
    if stepType == 'generic':
        return GenericStep(stepValue)
     
    raise ConfigException('Unknown step type: ' + stepType)

from wehi_pipeline.config.symbols import PreCommand        
from wehi_pipeline.config.symbols import Output

class JobStep(object):

    def __init__(self, name, outputs):
        self._name = name
        self._outputs = []
        if outputs is not None:
            if type(outputs) is not list:
                outputs = [outputs]
            for o in outputs:
                self._outputs.append(Output(o))
        
    def name(self):
        return self._name
        
    def outputs(self):
        return self._outputs

    def function(self):
        raise Exception('function is no implemented.')
    
class GenericStep(JobStep):
    
    def __init__(self, config):
        outputs = config['outputs'] if 'outputs' in config else None
        super(GenericStep, self).__init__(config['name'], outputs)
        
        self._preCommands = []
        if 'precommands' in config:
            for preCommand in config['precommands']:
                self._preCommands.append(PreCommand(preCommand))

        if 'modules' in config:
            self._modules = config['modules']
        else:
            self._modules = []
            
    def symbols(self):
            return dict()
        
    def function(self):
        def f(job):
            
            (cmds, outputFiles) = _prepareCommandLines(self._commands, self._config, self)
            
            execute(job, cmds, outputFiles)
            
        return f

def _isEmpty(thing):
    if thing is None:
        return True
    if type(thing) is list:
        return list == []
    return False

def _asList(l):
    if type(l) is list:
        return l
    else:
        return [l]
    
def _resolvePrecommands(preCommands):
    if _isEmpty(preCommands):
        return None

    resolvedCommands = []
    for preCommand in preCommand:
        pu = subprocess.check_output("zcat  forward | head -n1 | cut -d\":\" -f3,6,7 | sed 's/:/./g' | sed 's/\\s1//g'", shell=True)
        pu = pu.strip()
    
def _prepareCommandLines(commands, config, stepConfig):
    
    if _isEmpty(commands):
        return (None, None)
    
    commands = _asList(commands)
    
    processedCommands = []
    outFiles = []
    for cmd in commands:
        (processedCommand, fs) = _processCommandLine(cmd, config, stepConfig)
        processedCommands.append(processedCommand)
        outFiles = outFiles + fs
        
    return (processedCommand, outFiles)

def _processCommandLine(cmd, config, stepConfig, previousOutputFiles):
    tokens = _getTokens(cmd)
    
    if _isEmpty(tokens):
        return cmd
    
    outputFiles = []
    for token in tokens:
        symbol = _symbolFor(token, config, stepConfig)
        if symbol is None:
            raise ConfigException('Could not work out what the value of $' + token + ' should be.')
        
        symbol.resolve()
        value = symbol.value()
        if value is None:
            raise ConfigException('The symbol, ' + token + ', has no value.')
        
        cmd = _replaceToken(cmd, token, value)
        
        if type(symbol) is Output:
            outputFiles.append(value)
            
def _symbolFor(token, config, stepConfig, previousOutputFiles):
    return findSymbol(token, [previousOutputFiles, stepConfig.symbols(), config.symbols()])

def _replaceToken(cmd, token, value):
    return re.sub('\$'+token, value, cmd)
    
def _getTokens(s):
    tks = re.findall('\$[0-9a-zA-Z]*', s)
    tokenSet = set()
    for t in tks:
        tokenSet.add(t[:])
    return tokenSet


    