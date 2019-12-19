#!/usr/bin/python
__author__ = 'shastri@umass.edu'

from datetime import datetime
import os.path
import time

#------------------------------------------
# Globals initialized to default values
#------------------------------------------
autoMonitorApp_lxcName = 'autoLXC'
autoMonitorApp_lastTime = datetime.now()
autoMonitorApp_lastCPU = 0

#------------------------------------------
# Internal functions
#------------------------------------------

def getCpuTime():
    atTime = datetime.now()
    with open('/sys/fs/cgroup/cpu/lxc/autoLXC/cpuacct.usage', 'r') as cpuFile:
        curUsage = int(cpuFile.readline())
        return (atTime,curUsage)
#END getCpuTime


#------------------------------------------
# Public functions
# -- autoMonitorApp_init
# -- autoMonitorApp_getResourceLevel 
# -- autoMonitorApp_status
#------------------------------------------

def autoMonitorApp_init(lxcName):
    global autoMonitorApp_lastTime, autoMonitorApp_lastCPU, autoMonitorApp_lxcName
    autoMonitorApp_lxcName = lxcName

    atTime, curUsage = getCpuTime()
    autoMonitorApp_lastTime, autoMonitorApp_lastCPU = atTime, curUsage 
    print 'Initialized CPU reading (' + str(atTime) + ',' + str(curUsage) + ')'
    return True
#END autoMonitorAppInit


def autoMonitorApp_getResourceLevel():
    global autoMonitorApp_lastTime, autoMonitorApp_lastCPU
    
    prevTime, prevCPU = autoMonitorApp_lastTime, autoMonitorApp_lastCPU
    curTime, curCPU   = getCpuTime()

    timeDiff = curTime - prevTime
    timeDiffInNanosec = ((timeDiff.seconds * 1000000) + timeDiff.microseconds) * 1000
    curPercent = float(curCPU - prevCPU) / timeDiffInNanosec
    #print 'timeDiff = '  + str(timeDiffInNanosec) + ', cur = ' + str(curCPU) + ', prev = ' + str(prevCPU)
    
    autoMonitorApp_lastTime, autoMonitorApp_lastCPU = curTime, curCPU 
    
    return curPercent * 100 
#END autoMonitorApp_getResourceLevel


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("lxcName", help="Name of the container to be monitored")
    args = parser.parse_args()
    
    autoMonitorApp_init(args.lxcName)
    time.sleep(5)
    
    for i in range(1,5):
        print str(autoMonitorApp_getResourceLevel())
	time.sleep(i)


