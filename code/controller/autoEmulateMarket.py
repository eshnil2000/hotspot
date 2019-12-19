#!/usr/bin/python

import argparse
from datetime import datetime
import os 
import sys
import re
import autoMonitorMarket
import random

#------------------------------------------
# Globals initialized to default values
#------------------------------------------
MIN_MIGR_TIME = 5 * 60
autoEmulateMarket_mode = ''
autoEmulateMarket_bestMarketDict = dict()

# Dictionary of resource description 
# Format {'instanceType': [on-demand price, ECU, Memory (GiB)]}
marketDict = {
    'm1.small'      : [0.044, 1, 1.7],
    'm1.medium'     : [0.087, 2, 3.57],
    'm1.large'      : [0.175, 4, 7.5],
    'm1.xlarge'     : [0.35, 8, 15],
    'm3.medium'     : [0.067, 3, 3.75],
    'm3.large'      : [0.133, 6.5, 7.5],
    'm3.xlarge'     : [0.266, 13, 15], 
    'm3.2xlarge'    : [0.532, 26, 30],
    'm4.large'      : [0.12, 6.5, 8],
    'm4.xlarge'     : [0.239, 13, 16],
    'm4.2xlarge'    : [0.479, 26, 32],
    'm4.4xlarge'    : [0.958, 53.5, 64],
    'm4.10xlarge'   : [2.394, 124.5, 160],
    'c1.medium'     : [0.13, 5, 1.7],
    'c1.xlarge'     : [0.52, 20, 7],
    'cc2.8xlarge'   : [2, 88, 60.5],
    'c3.large'      : [0.105, 7, 3.75],
    'c3.xlarge'     : [0.21, 14, 7.5],
    'c3.2xlarge'    : [0.42, 28, 15],
    'c3.4xlarge'    : [0.84, 55, 30],
    'c3.8xlarge'    : [1.68, 108, 60],
    'c4.large'      : [0.105, 8, 3.75],
    'c4.xlarge'     : [0.209, 16, 7.5],
    'c4.2xlarge'    : [0.419, 31, 15],
    'c4.4xlarge'    : [0.838, 62, 30],
    'c4.8xlarge'    : [1.675, 132, 60],
    'd2.xlarge'     : [0.69, 14, 30.5],
    'd2.2xlarge'    : [1.38, 28, 61],
    'd2.4xlarge'    : [2.76, 56, 122],
    'd2.8xlarge'    : [5.52, 116, 224],
    'i2.xlarge'     : [0.853, 14, 30.5],
    'i2.2xlarge'    : [1.705, 27, 61],
    'i2.4xlarge'    : [3.41, 53, 122],
    'i2.8xlarge'    : [6.82, 104, 244],
    'g2.2xlarge'    : [0.65, 26, 15],  
    'g2.8xlarge'    : [2.6, 104, 60],
    'cg1.4xlarge'   : [2.1, 33.5, 22.5],
    'r3.large'      : [0.166, 6.5, 15],
    'r3.xlarge'     : [0.333, 13, 30.5],
    'r3.2xlarge'    : [0.665, 26, 61],
    'r3.4xlarge'    : [1.33, 52, 122],
    'r3.8xlarge'    : [2.66, 104, 244],
    'm2.xlarge'     : [0.245, 6.5, 17.1],
    'm2.2xlarge'    : [0.49, 13, 34.2],
    'm2.4xlarge'    : [0.98, 26, 68.4],
    'cr1.8xlarge'   : [3.5, 88, 244],
    'hi1.4xlarge'   : [3.1, 35, 60.5],
    't1.micro'      : [0.02, 0.5, 0.615]
}

# Setting the largest EC2 machine (m4.10xlarge) as the median server 
# for Google jobs. This is to avoid extraneous boost the jobs were
# getting from CPU scaling without taking IO into account.
refPrice, refCPU, refMem = 2*2.394, 2*124.5, 2*160 
random.seed()

def computeMigrationDuration(memSize):
    # Ref instance takes 640 seconds to migrate via RAM-fs transfer.
    # Every migration incurs a constant-time EBS/ENI move (28 sec)
    return (float(memSize) * 640) + 28
#[END computeMigrationDuration]


#------------------------------------------
# Internal functions
#------------------------------------------

def buildMarketState(startTime, endTime):
    # Build a tuple for every MIN_MIGR_TIME=300s, which is automaton's monitoring frequency as well.
    effMarketString, safeMarketString = '', ''

    for atTime in range(startTime, endTime, MIN_MIGR_TIME):
        effMarkets, safeMarkets = autoMonitorMarket.autoMonitorMarket_orderMarkets(atTime)
        
        marketCost = autoMonitorMarket.autoMonitorMarket_computeCost(effMarkets[0][0], atTime, atTime + MIN_MIGR_TIME)
        revcFlag, revcTime, revcPrice = autoMonitorMarket.autoMonitorMarket_checkRevc(
                                            effMarkets[0][0], atTime, atTime + MIN_MIGR_TIME)
        effMarketString += str(atTime) + ',' + str(effMarkets[0][0]) + ',' + str(marketCost) + ',' + str(revcFlag) + '\n'

        marketCost = autoMonitorMarket.autoMonitorMarket_computeCost(safeMarkets[0][0], atTime, atTime + MIN_MIGR_TIME)
        revcFlag, revcTime, revcPrice = autoMonitorMarket.autoMonitorMarket_checkRevc(
                                            safeMarkets[0][0], atTime, atTime + MIN_MIGR_TIME)
        safeMarketString += str(atTime) + ',' + str(safeMarkets[0][0]) + ',' + str(marketCost) + ',' + str(revcFlag) + '\n'

    with open('./effMarket.jan1.data', 'w') as outFp:
        outFp.write(effMarketString)
    
    with open('./safeMarket.jan1.data', 'w') as outFp:
        outFp.write(safeMarketString)
#END buildMarketState


def buildLookupTable(inputSrc):
    global autoEmulateMarket_bestMarketDict

    with open(inputSrc) as inFp:
        for line in inFp:
            atTime, market, cost, revc = line.split(',')
            autoEmulateMarket_bestMarketDict[int(atTime)] = [market, cost, revc]

#END buildLookupTable


#------------------------------------------
# Public functions
# -- autoEmulateMarket_init
# -- autoEmulateMarket_getBestMarket
#------------------------------------------

def autoEmulateMarket_init(mode, inputSrc):
    global autoEmulateMarket_mode
    autoEmulateMarket_mode = mode

    # Initialize autoMonitorMarket to gather 48 hours worth of spot price traces.
    if(mode == 'Build'):
        startTime, endTime = 0, (3600 * 24 * 5)
        autoMonitorMarket.autoMonitorMarket_init(inputSrc, startTime, endTime)
        buildMarketState(startTime, endTime)
    elif(mode == 'Lookup'):
        buildLookupTable(inputSrc)
        
    return True
#END autoEmulateMarket_init


def autoEmulateMarket_getBestMarket(inputTime):
    if autoEmulateMarket_mode != 'Lookup':
        return []

    atTime = int(inputTime)
    atTime = atTime - (atTime % MIN_MIGR_TIME) 
    if atTime not in autoEmulateMarket_bestMarketDict:
        print "Unaligned lookup time: " + str(atTime)
        return []
    else:
        return autoEmulateMarket_bestMarketDict[atTime]
#END autoEmulateMarket_getBestMarket


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", help="Build or Lookup emulator DB")
    parser.add_argument("inputSrc", help="Path to spot price directory or best markets file")
    args = parser.parse_args()
    
    autoEmulateMarket_init(args.mode, args.inputSrc)
    for atTime in range(0, 5*MIN_MIGR_TIME, MIN_MIGR_TIME/2):
        print autoEmulateMarket_getBestMarket(atTime)

