#!/usr/bin/python
__author__ = 'shastri@umass.edu'

import boto3
from datetime import datetime
import time

#------------------------------------------
# Globals initialized to default values
#------------------------------------------
autoMonitorMarket_region = ''

EC2ZoneDict = { 'us-west-1':['a', 'b', 'c'], 
                'us-west-2':['a', 'b', 'c'], 
                'us-east-1':['a', 'b', 'c', 'd', 'e'],
                'eu-west-1':['a', 'b', 'c'], 
                'eu-central-1':['a', 'b'],
                'ap-south-1':['a', 'b'], 
                'ap-southeast-1':['a', 'b'], 
                'ap-southeast-2':['a', 'b', 'c'],
                'sa-east-1':['a', 'b', 'c'], 
                'ap-northeast-1':['a', 'b', 'c'], 
                'ap-northeast-2':['a', 'c'] }

EC2InstList = ['t1.micro','t2.nano','t2.micro','t2.small','t2.medium','t2.large','m1.small','m1.medium','m1.large','m1.xlarge','m3.medium','m3.large','m3.xlarge','m3.2xlarge','m4.large','m4.xlarge','m4.2xlarge','m4.4xlarge','m4.10xlarge','m2.xlarge','m2.2xlarge','m2.4xlarge','cr1.8xlarge','r3.large','r3.xlarge','r3.2xlarge','r3.4xlarge','r3.8xlarge','x1.4xlarge','x1.8xlarge','x1.16xlarge','x1.32xlarge','i2.xlarge','i2.2xlarge','i2.4xlarge','i2.8xlarge','hi1.4xlarge','hs1.8xlarge','c1.medium','c1.xlarge','c3.large','c3.xlarge','c3.2xlarge','c3.4xlarge','c3.8xlarge','c4.large','c4.xlarge','c4.2xlarge','c4.4xlarge','c4.8xlarge','cc1.4xlarge','cc2.8xlarge','g2.2xlarge','g2.8xlarge','cg1.4xlarge','d2.xlarge','d2.2xlarge','d2.4xlarge','d2.8xlarge']

# Dictionary for fixed on-demand prices
OnDemandPriceDict = {
'm1.small':0.044,  'm1.medium':0.087,  'm1.large':0.175,   'm1.xlarge':0.35,
'm3.medium':0.067, 'm3.large':0.133,   'm3.xlarge':0.266,  'm3.2xlarge':0.532,
'm4.large':0.12,   'm4.xlarge':0.239,  'm4.2xlarge':0.479, 'm4.4xlarge':0.958, 'm4.10xlarge':2.394,
'c1.medium':0.13,  'c1.xlarge':0.52,   'cc2.8xlarge':2,
'c3.large':0.105,  'c3.xlarge':0.21,   'c3.2xlarge':0.42,  'c3.4xlarge':0.84,  'c3.8xlarge':1.68,
'c4.large':0.105,  'c4.xlarge':0.209,  'c4.2xlarge':0.419, 'c4.4xlarge':0.838, 'c4.8xlarge':1.675,
'd2.xlarge':0.69,  'd2.2xlarge':1.38,  'd2.4xlarge':2.76,  'd2.8xlarge':5.52,
'i2.xlarge':0.853, 'i2.2xlarge':1.705, 'i2.4xlarge':3.41,  'i2.8xlarge':6.82,
'g2.2xlarge':0.65, 'g2.8xlarge':2.6,   'cg1.4xlarge':2.1,
'r3.large':0.166,  'r3.xlarge':0.333,  'r3.2xlarge':0.665, 'r3.4xlarge':1.33,  'r3.8xlarge':2.66,
'm2.xlarge':0.245, 'm2.2xlarge':0.49,  'm2.4xlarge':0.98,  'cr1.8xlarge':3.5,
'hi1.4xlarge':3.1, 't1.micro':0.02}


#------------------------------------------
# Internal functions
#------------------------------------------
def getCurPriceFromCloud(instList = EC2InstList):
    utcStr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    productTypeList = ['Linux/UNIX', 'Linux/UNIX (Amazon VPC)']
    spotPriceDictForRegion = {}

    client = boto3.client('ec2', region_name = autoMonitorMarket_region)
    zoneList = EC2ZoneDict[autoMonitorMarket_region]
    
    for zoneSuffix in zoneList:
        spotPriceDictForZone = {}
        availZone = region + zoneSuffix

        response = client.describe_spot_price_history(
            StartTime = utcStr, 
            EndTime = utcStr, 
            InstanceTypes = instList,
            ProductDescriptions = productTypeList,
            AvailabilityZone = availZone,
        )
        
        # Check if we got a success response, else return
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            return spotPriceDictForRegion
        
        # Each instance-productType-zone combo comes as a seprate dictionary 
        # Process them one at a time from the list of responses
        for instPriceDict in response['SpotPriceHistory']:
            name = instPriceDict['InstanceType']
            isVpc = '.vpc' if 'VPC' in instPriceDict['ProductDescription'] else '' 
            price = instPriceDict['SpotPrice']
            spotPriceDictForZone[name + isVpc] = float(price)

        spotPriceDictForRegion[availZone] = spotPriceDictForZone

    return spotPriceDictForRegion
#END getCurPriceFromCloud


#------------------------------------------
# Public functions
# -- autoMonitorMarket_init
#------------------------------------------

def autoMonitorMarket_init(region):
    global autoMonitorMarket_region
    autoMonitorMarket_region = region
    return True
#END autoMonitorMarket_init


def autoMonitorMarket_getCurPrice(instList = EC2InstList):
    return getCurPriceFromCloud(instList)
#END autoMonitorMarket_getCurPrice


#------------------------------------------
# Unit test 
#------------------------------------------

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("region", help="AWS region")
    args = parser.parse_args()
    
    if autoMonitorMarket_init(args.region) == True:
        print 'auto market monitor connected to ' + args.region
 
    startTime = datetime.now()
    spotPriceDictForRegion = autoMonitorMarket_getCurPrice(EC2InstList)
    queryTime = datetime.now() - startTime
    queryTimeMicrosec = (queryTime.seconds * 1000000) + queryTime.microseconds
        
    marketCount = 0
    for zone, zoneDict in spotPriceDictForRegion.items():
        marketCount += len(zoneDict)
	print 'zone = ' + zone
	print zoneDict
    print args.region + ' has ' + str(marketCount) + ' markets. Query time = ' + str(queryTimeMicrosec) + ' microsec'

