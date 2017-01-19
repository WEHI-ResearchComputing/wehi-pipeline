'''
Created on 27Sep.,2016

@author: thomas.e
'''

import threading
import datetime
import sys
import Queue
import logging

class StreamLogger:
    class Writer:
        def __init__(self, logger, level):
            self.level = level
            self.logger = logger
        
        def write(self, data):
            self.logger.ioQueue.put('[' + self.level + '] ' + data)
            
        def flush(self):
            self.logger.flush()
    
    def __init__(self, level, desc):
        self.level = level
        
        msg = '======>>>>>>> Log for ' + desc + ' opened at: {:%Y-%m-%d %H:%M:%S}'
        now = msg.format(datetime.datetime.now())
        self._emit(now)
        
        self.ioLock = threading.Lock()
        
        self.ioQueue = Queue.Queue()
        
        t = threading.Thread(target=self.ioMonitor, args=(self.ioQueue,))
        t.daemon = True
        t.start()
        
    def _emit(self, msg):
        logging.log(level=self.level, msg=msg)
        
    def ioMonitor(self, ioQueue):
        try:
            while True:
                line = self.ioQueue.get()
                self._emit(line)
                self.ioQueue.task_done()
        except:
            # This doesn't work - message is lost, thread hangs, application hangs
            logging.critical(' Failure in stream monitor: ' + sys.exc_info())
            
    def redirecter(self, level):
        return self.Writer(self, level)
    
    def redirect(self, level, stream):
        if stream is None: return
        
        t = threading.Thread(target=self.streamCopy, args=(level, stream))
        t.daemon = True
        t.stream = stream
        t.start()
        
    def streamCopy(self, level, stream):
        for line in iter(stream.readline, b''):
            line = line.decode('utf-8').strip('\n')
            self.ioQueue.put('[' + level + '] ' + line)
            
    def log(self, level, msg):
        logging.log(level=level, msg=('[' + level+ '] ' + msg))
        
    def shutdown(self):
        self.ioQueue.join()
