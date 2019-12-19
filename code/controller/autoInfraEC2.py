#!/usr/bin/python
__author__ = 'shastri@umass.edu'

import boto3
import time
import boto.utils
import os

#------------------------------------------
# Globals initialized to default values
#------------------------------------------
autoInfraEC2_mode = 'NULL'

amiDict = {'us-west-1': 'ami-f7f0ad97', 'us-east-1': '',
           'eu-west-1': '', 'ap-northeast-1': ''}

#------------------------------------------
# Internal functions
#------------------------------------------

def checkEC2AccountStatus():
    # Query the account attributes
    client = boto3.client('ec2', region_name = 'us-east-1')
    attrResponse = client.describe_account_attributes(
        AttributeNames = ['supported-platforms']
    )

    # Process the response
    if attrResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        return False

    attrList = attrResponse['AccountAttributes']
    print 'Supported platforms are: '
    for item in (attrResponse['AccountAttributes'][0])['AttributeValues']:
        print item['AttributeValue']
    return True
#END checkEC2AccountStatus


rootDeviceList = [ {'DeviceName': '/dev/sdb',
                    'Ebs': {'VolumeSize': 80, 'DeleteOnTermination': True,
                            'VolumeType': 'gp2', 'Encrypted': False },
                   } ] 


def acquireStdInstance(instFamily, instZone):
    instRegion = instZone[:-1]
    client = boto3.client('ec2', region_name = instRegion)
    stdResponse = client.run_instances(
                      ImageId = amiDict[instRegion],
                      MinCount = 1,
                      MaxCount = 1,
                      KeyName = 'Automaton',
                      InstanceType = instFamily,
                      Placement = {'AvailabilityZone': instZone},
                      BlockDeviceMappings = rootDeviceList,
                      Monitoring = {'Enabled': False},
                      InstanceInitiatedShutdownBehavior = 'terminate',
                  )

    # Process the response
    if stdResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        return 'i-failed'

    # Extract spotReqId and wait for it to be processed
    stdResponseDict = stdResponse['Instances'][0]
    instId = stdResponseDict['InstanceId']
    instState = stdResponseDict['State']['Name']
    print 'Inst id: ' + instId + ' is ' + instState

    waiter = client.get_waiter('instance_running')
    waiter.wait(InstanceIds = [instId])

    stdResponse = client.describe_instance_status(InstanceIds = [instId])

    if stdResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        return 'i-failed'

    stdResponseDict = stdResponse['InstanceStatuses'][0]
    instState = stdResponseDict['InstanceState']['Name']
    if instState != 'running':
        return 'i-failed'
    
    print 'Std instance ' + instId + ' is ' + instState
    return instId

#END acquireStdInstance


def acquireSpotInstance(instFamily, instZone, instBid):
    instRegion = instZone[:-1]
    client = boto3.client('ec2', region_name = instRegion)
    spotResponse = client.request_spot_instances(
                        SpotPrice = str(instBid),
                        InstanceCount = 1,
                        Type = 'one-time',
                        LaunchSpecification = {
                            'ImageId': amiDict[instRegion],
                            'KeyName': 'Automaton',
                            'InstanceType': instFamily,
                            'Placement': {'AvailabilityZone': instZone},
                            'BlockDeviceMappings': rootDeviceList
                        }
                    )

    # Process the response
    if spotResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        return 'i-failed'

    # Extract spotReqId and wait for it to be processed
    spotResponseDict = spotResponse['SpotInstanceRequests'][0]
    spotReqId = spotResponseDict['SpotInstanceRequestId']
    spotReqStatus = spotResponseDict['Status']['Code']
    print 'Spot Request id: ' + spotReqId + ', Status: ' + spotReqStatus

    waiter = client.get_waiter('spot_instance_request_fulfilled')
    waiter.wait(SpotInstanceRequestIds = [spotReqId])
   
    # Request should be processed now. Check if fulfilled.
    spotResponse = client.describe_spot_instance_requests(
        SpotInstanceRequestIds = [spotReqId]
    )
    
    if spotResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        return 'i-failed'
    
    spotResponseDict = spotResponse['SpotInstanceRequests'][0]
    if spotResponseDict['Status']['Code'] != 'fulfilled':
        print 'Spot request ' + spotResponseDict['Status']['Code']
        return 'i-failed' 

    spotId = spotResponseDict['InstanceId']
    spotTime = spotResponseDict['CreateTime']
    spotState = spotResponseDict['State']
    print 'Created spot instance ' + spotId + ' at ' + str(spotTime) + ', current state is ' + spotState

    # Now, we wait until the instance is in running state.
    waiter = client.get_waiter('instance_running')
    waiter.wait(InstanceIds = [spotId])

    spotResponse = client.describe_instance_status(InstanceIds = [spotId])

    if spotResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        return 'i-failed'

    spotResponseDict = spotResponse['InstanceStatuses'][0]
    spotState = spotResponseDict['InstanceState']['Name']
    if spotState != 'running':
        return 'i-failed'
    
    print 'Spot instance ' + spotId + ' is ' + spotState
    return spotId

#END acquireSpotInstance


def terminateInstance(instZone, instId):
    instRegion = instZone[:-1]
    client = boto3.client('ec2', region_name = instRegion)
    termResponse = client.terminate_instances(InstanceIds = [instId])

    # Process the response
    if termResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        return False

    termResponseDict = termResponse['TerminatingInstances'][0]
    state = termResponseDict['CurrentState']['Name']
    print 'Instance state changed to: ' + state

    # Check if the instance has been terminated
    waiter = client.get_waiter('instance_terminated')
    waiter.wait(InstanceIds = [instId])

    termResponse = client.describe_instances(
                        InstanceIds = [instId]
                    )

    termResponseDict = termResponse['Reservations'][0]
    instState = termResponseDict['Instances'][0]['State']['Name']
    if instState != 'terminated':
        return False
    
    print 'Instance ' + instId + ' is ' + instState
    return True
#END terminateInstance


#------------------------------------------
# Public functions
# -- autoInfraEC2_init
# -- autoInfraEC2_acquireInstance
# -- autoInfraEC2_deleteInstance
# -- autoInfraEC2_status 
#------------------------------------------

def autoInfraEC2_init(inputType = 'CLOUD', inputVal = 'NULL'):
    if type == 'SIMULATE':
        autoInfraEC2_mode = 'SIMULATE'
    else:
        #if checkEC2AccountStatus() == True:
        autoInfraEC2_mode = 'CLOUD'

    return True 
#END autoInfraEC2Init


def autoInfraEC2_acquireInstance(instFamily, instZone, instType = 'on-demand', instBid = 0.0):
    if autoInfraEC2_mode == 'SIMULATE':
        return 'i-simulate'

    if instType == 'on-demand':
        return acquireStdInstance(instFamily, instZone)
    else:
        return acquireSpotInstance(instFamily, instZone, instBid)
#END autoInfraEC2_acquireInstance


def autoInfraEC2_deleteInstance(instZone, instID):
    if autoInfraEC2_mode == 'SIMULATE':
        return True 

    return terminateInstance(instZone, instID)
#END autoInfraEC2_deleteInstance


def autoInfraEC2_status():
    print 'EC2 infra mode: ' + autoInfraEC2_mode
    # Parse the data structures and print summary
#END autoInfraEC2_status

gEC2EbsId = 'vol-05a36f8913c2dd8f1'
gEC2EniId = 'eni-7678a579'

def autoInfraEC2_moveEBS(instZone, ec2InstId, ec2MigrInstId):
    #result = os.system('/usr/bin/sudo /home/ubuntu/automaton/autoScriptCleanup.sh')
    #if result != 0:
    #    return False

    instRegion = instZone[:-1]
    client = boto3.client('ec2', region_name = instRegion)
    
    # Detach the EBS volume
    response = client.detach_volume(
                    VolumeId = gEC2EbsId,
                    Force = True
                )
    print 'EBS volume ' + gEC2EbsId + ' is ' + response['State']
    
    # Wait until the EBS is completely detached.
    waiter = client.get_waiter('volume_available')
    waiter.wait(VolumeIds = [gEC2EbsId])
    print 'EBS volume in Available state'

    # Attach the EBS volume and wait till it is in-use.
    response = client.attach_volume(
                    VolumeId = gEC2EbsId,
                    InstanceId = ec2MigrInstId,
                    Device = '/dev/sdh',
                )
    print 'EBS volume ' + gEC2EbsId + ' is ' + response['State']
   
    waiter = client.get_waiter('volume_in_use')
    waiter.wait(VolumeIds = [gEC2EbsId])
    print 'EBS volume is in-use'
#END autoInfraEC2_moveEBS


def autoInfraEC2_moveEbsAndEni(instZone, ec2InstId, ec2MigrInstId):
    result = os.system('/usr/bin/sudo /home/ubuntu/automaton/autoScriptCleanup.sh')
    if result != 0:
        return False

    client = boto3.client('ec2', region_name = instZone[:-1])
    
    # Detach the EBS volume
    response = client.detach_volume(
                    VolumeId = gEC2EbsId,
                    InstanceId = ec2InstId,
                    Device = '/dev/sdh',
                    Force = True
                )
    print 'EBS volume ' + gEC2EbsId + ' is ' + response['State']

    # Get the ENI attachment id, and then detach the interface
    response = client.describe_network_interface_attribute(
                    NetworkInterfaceId = gEC2EniId,
                    Attribute = 'attachment'
                )
    eniAttachmentId = response['Attachment']['AttachmentId']
    print 'ENI attachment id: ' + eniAttachmentId

    response = client.detach_network_interface(
                    AttachmentId = eniAttachmentId,
                    Force = True
                )
    print 'ENI interface ' + gEC2EniId + ' is ' + response['State']
    
    # Wait until the EBS is completely detached.
    waiter = client.get_waiter('volume_available')
    waiter.wait(VolumeIds = [gEC2EbsId])
    print 'EBS volume in Available state'

    # Wait until the ENI is completely detached.
    waiter = client.get_waiter('network_interface_available')
    waiter.wait(NetworkInterfaceIds = [gEC2EniId])
    print 'ENI interface in Available state'

    # First attach the network interface to the new instance.
    response = client.attach_network_interface(
                    NetworkInterfaceId = gEC2EniId,
                    InstanceId = ec2MigrInstId,
                    DeviceIndex = 1
                )
    newEniAttachmentId = response['AttachmentId']
    print 'New ENI attachment id: ' + newEniAttachmentId
 
    # Attach the EBS volume and wait till it is in-use.
    response = client.attach_volume(
                    VolumeId = gEC2EbsId,
                    InstanceId = ec2MigrInstId,
                    Device = '/dev/sdh',
                )
    print 'EBS volume ' + gEC2EbsId + ' is ' + response['State']
   
    waiter = client.get_waiter('volume_in_use')
    waiter.wait(VolumeIds = [gEC2EbsId])
    print 'EBS volume is in-use'
#END autoInfraEC2_moveEBS


def autoInfraEC2_setupEbsAndEni():
    result = os.system('sudo /home/ubuntu/autoScriptSetup.sh')
#END autoInfraEC2_setupEBSandENI


def autoInfraEC2_getCurInstId():
    return boto.utils.get_instance_metadata()['instance-id']
#END autoInfraEC2_getCurInstId


#------------------------------------------
# Microbenchmark functions 
# -- autoTest_acquireDelete
# -- autoTest_moveEbsEni
#------------------------------------------

def autoTest_acquireDelete(availZone):
    startTime = time.time()
    stdInstId = autoInfraEC2_acquireInstance('m3.large', availZone, 'on-demand')
    endTime = time.time()
    print 'Acquired STD inst ' + str(stdInstId) + ' in ' + availZone + ' in ' + str(endTime - startTime) + ' sec'
    
    startTime = time.time()
    spotInstId = autoInfraEC2_acquireInstance('m3.large', availZone, 'spot', 1.0)
    endTime = time.time()
    print 'Acquired SPOT inst ' + str(spotInstId) + ' in ' + availZone + ' in ' + str(endTime - startTime) + ' sec'

    time.sleep(60)

    startTime = time.time()
    autoInfraEC2_deleteInstance(availZone, stdInstId)
    endTime = time.time()
    print 'Terminated STD inst ' + str(stdInstId) + ' in ' + str(endTime - startTime) + ' sec'

    startTime = time.time()
    autoInfraEC2_deleteInstance(availZone, spotInstId)
    endTime = time.time()
    print 'Terminated SPOT inst ' + str(spotInstId) + ' in ' + str(endTime - startTime) + ' sec'
    
#END autoTest_acquireDelete


def autoTest_moveEbsEni(availZone):
    startTime = time.time()
    autoInfraEC2_moveEbsAndEni(availZone)
    endTime = time.time()
    print 'Moved EBS and ENI in ' + str(endTime - startTime) + ' sec'
#END autoTest_moveEbsEni


def autoTest_migrate(availZone):
    curInstId = autoInfraEC2_getCurInstId()
    migrInstId = autoInfraEC2_acquireInstance('m3.large', availZone, 'on-demand')
    autoInfraEC2_moveEbsAndEni(availZone, curInstId, migrInstId)
#END autoTest_migrate


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("ec2Mode",  help="Use Cloud APIs or simulate")
    parser.add_argument("availZone", help="Availability Zone")
    args = parser.parse_args()
    
    if autoInfraEC2_init(args.ec2Mode) == True:
        print 'auto infra is connected to ' + args.ec2Mode

#    autoTest_acquireDelete(args.availZone)
#    autoTest_moveEbsEni(args.availZone)
    autoTest_migrate(args.availZone)
