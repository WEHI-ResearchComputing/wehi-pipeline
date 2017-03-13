'''
Created on 17Feb.,2017

@author: thomas.e
'''

from __future__ import division
import string 

import os
import subprocess

def addModules(modules):
    env = os.environ
    
    if modules is None:
        return env
    
    if type(modules) is not list:
        modules = [modules]
        
    for module in modules:
        env = _addModule(env, module)
        
    return env

def _addModule(env, module):
    
    actions = _moduleActions(module)
    
    if actions is None:
        return env
    
    for action in actions:
        action(env)
        
    return env

def _moduleActions(module):
    crap = subprocess.check_output(['/usr/bin/modulecmd', 'bash', 'show', module],stderr=subprocess.STDOUT)
    lines = crap.split('\n')
    
    actions = []
    for line in lines:
        words = line.split()
        action = _actionFor(words)
        if action is not None:
            actions.append(action)
        
    return actions
    
def prepend(words):
    variable = words[0]
    value = words[1]
    
    def a(env):
        if variable in env:
            env[variable] = value + ':' + env[variable]
        else:
            env[variable] = value
    
    return a  
    
def setenv(words):
    variable = words[0]
    value = words[1]
    
    def a(env):
        env[variable] = value
    
    return a  

def unsetenv(words):
    variable = words[0]
    
    def a(env):
        if variable in env:
            del env[variable]
            
    return a

_actions = {
    'prepend-path' : prepend,
    'unsetenv' : unsetenv,
    'setenv' : setenv
    }

def _actionFor(words):
    if words == []:
        return None
    
    action = words[0]
    
    if action in _actions:
        return _actions[action](words[1:])
    else:
        return None

def javaHelper(modules, cmd):
#     import pydevd
#     pydevd.settrace('10.1.17.53', stdoutToServer=True, stderrToServer=True, suspend=False)
    
    env = addModules(modules)
    
    fn = _which(cmd, env)
    
    if fn is None:
        return None
    
    with open(fn) as fh:
        for line in fh:
            words = line.split()
            if words is None:
                continue
            l = len(words)
            indx = words.index('-jar')
            if indx > -1 and l-1 > indx:
                return words[indx+1]
            
    return None
 
def _which(program, env):
    # http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
    
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, _ = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in env["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None

def _isText(filename):
    # http://stackoverflow.com/questions/1446549/how-to-identify-binary-and-text-files-using-python

    s=open(filename).read(512) # I assume this will close the file!!
    text_characters = "".join(map(chr, range(32, 127)) + list("\n\r\t\b"))
    _null_trans = string.maketrans("", "")
    if not s:
        # Empty files are considered text
        return True
    if "\0" in s:
        # Files with null bytes are likely binary
        return False
    # Get the non-text characters (maps a character to itself then
    # use the 'remove' option to get rid of the text characters.)
    t = s.translate(_null_trans, text_characters)
    # If more than 30% non-text characters, then
    # this is considered a binary file
    if float(len(t))/float(len(s)) > 0.30:
        return False
    return True