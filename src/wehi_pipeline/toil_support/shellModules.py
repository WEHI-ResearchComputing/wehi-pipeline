'''
Created on 17Feb.,2017

@author: thomas.e
'''

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
