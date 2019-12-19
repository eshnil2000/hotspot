#!/usr/bin/python

from datetime import datetime
import os, sys, re, time 
import subprocess
import autoEmulateMarket, autoInfraEC2, autoMonitorApp 

MIN_MIGR_TIME = 5 * 60


def startController(confDict):
    # 1. Determine current time offset (rounded to nearest MIN_MIGR_TIME).
    #    If this is the first time, record current time in the conf file.
    if not confDict:
        timeStr = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        confDict['startTime'] = timeStr
        confDict['runCost'] = 0.0
        atTime = 0
        marketVector = autoEmulateMarket.autoEmulateMarket_getBestMarket(atTime)
        confDict['spotMarket'] = marketVector[0]
    else:
        confTime = datetime.strptime(confDict['startTime'],"%Y-%m-%dT%H:%M:%S")
        atTime = (datetime.now() - confTime).seconds
        atTime = atTime - (atTime + int(MIN_MIGR_TIME/2)) % MIN_MIGR_TIME
    
    curMarket = confDict['spotMarket']
    curCost = float(confDict['runCost'])
    migrFlag = False
    logStr = ''

    # 2. Start the controller loop
    #    a. Monitor container state. If dormant, exit controller.
    #    b. Monitor market (as per policy) and determine next destination.
    #    c. If migration is not required, sleep until next iteration.
    #    d. If migration required, update confFile and trigger migration.
    while(True):
        loopStartTime = datetime.now()
        cpuUtil = autoMonitorApp.autoMonitorApp_getResourceLevel()
        print 'Automaton at ' + str(cpuUtil) + ' %cpu'
        if cpuUtil < 5:
            logStr += 'CPU utilization hit ' + str(cpuUtil) + '... Terminating\n'
            break
        
        marketVector = autoEmulateMarket.autoEmulateMarket_getBestMarket(atTime)
        if marketVector == []:
            logStr += 'Got empty market vector... Terminating\n'
            break

        nextMarket, nextCost, revc = marketVector[0], float(marketVector[1]), marketVector[2]
        logStr += str(atTime) + ': got ' + str(marketVector) + '\n'
        curCost += nextCost

        if(revc == 'True'):
            logStr += '****** Revoked... need to restart the job ********\n'
            break

        if(nextMarket != curMarket):
            migrFlag = True
            break
            
        loopTime = (datetime.now() - loopStartTime).seconds
        time.sleep(MIN_MIGR_TIME - loopTime)
	atTime += MIN_MIGR_TIME

    # Done with the controller loop. Update conf file and log file
    with open('/home/ubuntu/automaton/src/autoLog', 'a') as logFp:
        logFp.write(logStr)

    confDict['spotMarket'] = curMarket
    confDict['runCost'] = str(curCost)
    confDict['endTime'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    with open('/home/ubuntu/automaton/src/autoConfig', 'w') as confFp:
        for key, value in confDict.items():
            confFp.write(str(key) + ',' + str(value) + '\n')
    
    # Trigger migration, and terminate self. 
    if migrFlag == True:
        subprocess.call(['/home/ubuntu/automaton/autoScriptCleanup.sh'])
        
        nextMarket = re.sub('\.us-west-1[abcde]', '', nextMarket)
        nextMarket = re.sub('\.vpc', '', nextMarket)
        
        curInstId = autoInfraEC2.autoInfraEC2_getCurInstId()
        migrInstId = autoInfraEC2.autoInfraEC2_acquireInstance(nextMarket, 'us-west-1c', 'on-demand')
        autoInfraEC2.autoInfraEC2_moveEBS('us-west-1c', curInstId, migrInstId)
        autoInfraEC2.autoInfraEC2_deleteInstance('us-west-1c', curInstId)

# END startController


if __name__ == '__main__':
    # Read the config file to get init information
    confDict = dict()
    with open('/home/ubuntu/automaton/src/autoConfig') as confFp:
        for line in confFp:
            line = line.rstrip('\n')
            v1, v2 = line.split(',')
            confDict[v1] = v2
    print confDict

    autoMonitorApp.autoMonitorApp_init('autoLXC')
    autoEmulateMarket.autoEmulateMarket_init('Lookup', '/home/ubuntu/automaton/src/markets.data')
    time.sleep(5)
    startController(confDict)
 
