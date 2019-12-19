#!/usr/bin/python
__author__ = 'shastri@umass.edu'

import argparse
from datetime import datetime
import os 
import sys
import operator
import re

# Dictionary of resource description 
# Format {'instanceType': [on-demand price, ECU, Memory (GiB)]}
onDemandMarketDict = {
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

spotUpdateDict = dict()
gBeginTime = 1483142400 + (0 * 86400) # Jan-1 + additional days

def getEpochTime(spotTime):
    pattern = '%Y-%m-%dT%H:%M:%S'
    utcTime = datetime.strptime(spotTime, pattern)
    epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
    return epochTime
# [END getEpochTime]


# Use timestamp as key from the priceTrace lines.
def getKey(priceUpdateLine):
    v1, price, time, inst, v5, v6 = priceUpdateLine.split('\t')
    return getEpochTime(time)
# [END getKey]


# spotMarket is in format instType.zone. Eg: "m1.large.vpc-us-east-1a" 
def autoMonitorMarket_checkRevc(spotMarket, startTime, endTime):
    if spotMarket not in spotUpdateDict:
        print "Market " + spotMarket + " doesnt exist"
        return (False, 0, 0)
    
    # Expecting startTime and endTime to be offset from gBeginTime.
    startTime += gBeginTime
    endTime += gBeginTime

    inst = re.sub('\.us-west-1[abcde]', '', spotMarket)
    inst = re.sub('\.vpc', '', inst)
    onDemandPrice = float(onDemandMarketDict[inst][0])
    
    # Go through all the list entries of the selected market
    for updateEntry in spotUpdateDict[spotMarket]:
        if updateEntry[0] > endTime: 
            break
        elif updateEntry[0] < startTime:
            continue
        else:
            if updateEntry[1] > onDemandPrice:
                return (True, updateEntry[0] - gBeginTime, updateEntry[1])

    return (False, 0, 0)
# END autoMonitorMarket_checkRevc


def autoMonitorMarket_orderMarkets(atTime, allowedMarkets = onDemandMarketDict):
    # All times are offset from gBeginTime.
    atTime += gBeginTime
    efficientMarkets = []
    safeMarkets = []

    # Extract the current price of each market.
    # Compare it to the known cheapest market.
    for spotMarket, updateList in spotUpdateDict.items():
        inst = re.sub('\.us-west-1[abcde]', '', spotMarket)
        inst = re.sub('\.vpc', '', inst)
        
        # Initialize the earliest spot price
        curPrice, prevPrice = updateList[0][1], updateList[0][1]
        for updateEntry in updateList:
            curPrice, curTime = updateEntry[1], updateEntry[0]
            if atTime < curTime:
                break
            else:
                continue
            prevPrice = curPrice

        # Found the price for <atTime>
        instEff =  prevPrice / float(onDemandMarketDict[inst][1])
        efficientMarkets += [(spotMarket, instEff)]
        
        priceRatio = prevPrice / onDemandMarketDict[inst][0]
        safeMarkets += [(spotMarket, priceRatio)]

    efficientMarkets.sort(key=operator.itemgetter(1), reverse=False)
    safeMarkets.sort(key=operator.itemgetter(1), reverse=False)
    return efficientMarkets, safeMarkets 
#END autoMonitorMarket_orderMarkets


def autoMonitorMarket_findEquivalentSet(efficientMarkets, safeMarkets):
    rankDict = {}
    #effWeight, riskWeight = 0.0, 1.0
    effWeight, riskWeight = 0.5, 0.5
    bestEff, worstEff = efficientMarkets[0][1], efficientMarkets[-1][1]
    bestRatio, worstRatio = safeMarkets[0][1], safeMarkets[-1][1]
    
    eqSetList = []
    eqSetSize = 10

    for market, eff in efficientMarkets:
        normalizedEff = (eff - bestEff) / (worstEff - bestEff)
        rankDict[market] = normalizedEff * effWeight

    for market, ratio in safeMarkets:
        normalizedRatio = (ratio - bestRatio) / (worstRatio - bestRatio)
        rankDict[market] = rankDict[market] + (normalizedRatio * riskWeight)

    # Convert the rankDict into a list, sort it and extract the 
    # required number of markets. We just return the market names
    # and not the rank associated with each.
    rankedList = [ [market,rank] for market,rank in rankDict.items() ]
    rankedList.sort(key=operator.itemgetter(1), reverse=False)
    eqSetList = [ item[0] for item in rankedList[:eqSetSize] ]
    return eqSetList
# END autoMonitorMarket_findEquivalentSet


# spotMarket is in format instType.zone. Eg: "m1.large.vpc-us-east-1a" 
def autoMonitorMarket_computeCost(spotMarket, startTime, endTime):
    if spotMarket not in spotUpdateDict:
        print "Market " + spotMarket + " doesnt exist"
        return 0 
    
    # Expecting startTime and endTime to be offset from gBeginTime.
    startTime += gBeginTime
    endTime += gBeginTime
    
    totalCost = 0.0
    firstUpdateList = spotUpdateDict[spotMarket][0]
    curPrice, curTime = firstUpdateList[1], firstUpdateList[0]

    # Iterate through the list entries of the selected market
    # until we reach the startTime 
    for updateEntry in spotUpdateDict[spotMarket]:
        prevPrice, prevTime = curPrice, curTime
        curPrice, curTime   = updateEntry[1], updateEntry[0]
    
        # Haven't yet reached the startTime, continue
        if startTime >= curTime:
            continue
        # Reached the endTime, finish up and return
        elif curTime >= endTime:
            # Check for the corner case when startTime and endTime
            # fall in the same update period
            if (startTime > prevTime):
                totalCost = (endTime - startTime) * (prevPrice / 3600.0)
            else:
                totalCost += (endTime - prevTime) * (prevPrice / 3600.0)
            break
        # We're in the active area, compute cost of prev cycle
        else:
            # For the first time, account only from startTime.
            # All subsequent iterations are whole periods.
            if totalCost == 0:
                totalCost = (curTime - startTime) * (prevPrice / 3600.0)
            else:
                totalCost += (curTime - prevTime) * (prevPrice / 3600.0)
        
    return totalCost
# END autoMonitorMarket_computeCost

# startTime and endTime default to time 0s and 3600s respectively.
def autoMonitorMarket_init(spotPriceDir, startTime = 0, endTime = 3600):
    global spotUpdateDict
    
    # Expecting startTime and endTime to be offset from gBeginTime.
    startTime += gBeginTime
    endTime += gBeginTime
    
    # Read all the EC2 price trace files into a dictionary of markets.
    # Each market would have a sorted list of price updates.
    for spotFile in os.listdir(spotPriceDir):
        spotFile = spotPriceDir + spotFile
        if os.path.isfile(spotFile):
            spotUpdates = open(spotFile).read().splitlines()
            sortedUpdates = sorted(spotUpdates, key=getKey)

            # Set/Update the gBeginTime based on the first entry.
            #if getKey(sortedUpdates[0]) > gBeginTime:
            #    gBeginTime = getKey(sortedUpdates[0]) 
            
            v1, price, time, inst, vpcInfo, zone = sortedUpdates[0].split('\t')
            spotMarket = inst + '.vpc.' + zone if 'VPC' in vpcInfo else inst + '.' + zone

            # Iterate through sorted updates and fillup the dict entry
            for update in sortedUpdates:
                v1, price, time, inst, v5, zone = update.split('\t')
                spotTime = getEpochTime(time)
                spotPrice = float(price)

                if spotTime < startTime:
                    prevTime, prevPrice = spotTime, spotPrice
                    continue
                
                elif spotTime > endTime:
                    break
                
                else:
                    if spotMarket not in spotUpdateDict:
                        spotUpdateDict[spotMarket] = [[prevTime, prevPrice]] + [[spotTime, spotPrice]]
                    else:
                        spotUpdateDict[spotMarket] = spotUpdateDict[spotMarket] + [[spotTime, spotPrice]]
            
            # Print if the market got entered in the spotUpdateDict
            if spotMarket in spotUpdateDict:
                print "Total " + str(len(spotUpdateDict[spotMarket])) + " for " + spotMarket

    print "-----------------------------------------"
    print 'gBeginTime =' + str(gBeginTime)
    print "-----------------------------------------"
    return True
# END autoMonitorMarket_init
 

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("spotPriceDir", help="Path to spot price directory")
    args = parser.parse_args()

    # Parse the price traces, and build data structures
    startTime = gBeginTime
    endTime = gBeginTime + (3600 * 24 * 2)
    if autoMonitorMarket_init(args.spotPriceDir, startTime, endTime) == True:
        print 'auto market monitor connected to PRICE-TRACES'
    else:
        print 'Failed initializaiton'
        exit()

    # Run for 4 days
    eqSetDict = {}
    for atTime in range(0, 3600 * 24 * 2, 300):
        efficientMarkets, safeMarkets = autoMonitorMarket_orderMarkets(atTime)
        eqSetList = autoMonitorMarket_findEquivalentSet(efficientMarkets, safeMarkets)
        eqSetDict[atTime] = eqSetList
        print "atTime: " + str(atTime)
        print eqSetList

