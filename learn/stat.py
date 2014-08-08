#! /usr/bin/python

import threading
import signal
class StatsCollector:
    shareLock=threading.Lock()
    stopRequestEvent=threading.Event()
    statsDB={'n':0}
    def collectDBStats(self):
        with self.shareLock:
            print 'in collecting:'
            self.statsDB['n']+=1
    def __init__(self,updateInterval=2):
        self.updateInterval=updateInterval
        self.workerThread=threading.Thread(target=self.working)
        self.workerThread.start()
    def working(self):
        while not self.stopRequestEvent.is_set():
            self.collectDBStats()
            self.stopRequestEvent.wait(self.updateInterval)
    def __del__(self):
        self.stop()
    def stop(self):
        self.stopRequestEvent.set()
        self.workerThread.join()
    def getStats(self):
        with self.shareLock:
            return self.statsDB

            
if __name__=='__main__':
    stopRequest=threading.Event()
    def signal_handler(signal,frame):
        print('Ctrl+C received! exiting..')
        stopRequest.set()
    signal.signal(signal.SIGINT,signal_handler)
    collector=StatsCollector()
    while not stopRequest.is_set() and collector.getStats()['n']<10:
        print 'current val: %d' % collector.getStats()['n']
        stopRequest.wait(1)
    collector.stop()
    print 'final val: %d' % collector.getStats()['n']