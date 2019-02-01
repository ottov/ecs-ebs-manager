 #!/usr/bin/env python
from __future__ import print_function

import boto3, botocore.exceptions
import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from botocore.config import Config

config = Config(
    retries = dict(
        max_attempts = 60
    )
)

ec2client = boto3.client('ec2',config=config) #,region_name='us-east-1') # Set AWS_DEFAULT_REGION in env

VOLUME_TYPE = 'gp2'

def getEC2_Zone():
    s = requests.Session()
    retries = Retry(total=5,
                backoff_factor=3,
                status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))

    r = s.get('http://169.254.169.254/latest/meta-data/placement/availability-zone', timeout=30)
    if not r.text:
        return None
    if r.status_code == 504:
        return None
    return r.text

def getEC2_InstanceId():
    s = requests.Session()
    retries = Retry(total=5,
                backoff_factor=3,
                status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))

    r = s.get('http://169.254.169.254/latest/meta-data/instance-id', timeout=30)
    if not r.text:
        return None
    if r.status_code == 504:
        return None
    return r.text


def createEBS(sz=42):
    """
    Create a new EBS and attach to local host
    :param sz: requested size in GB
    :return: volume_id
    """
    az = getEC2_Zone()

    if sz > 16384: sz = 16384
    if sz < 1: sz = 1
    res = ec2client.create_volume(
            AvailabilityZone = az,
            Encrypted = False,
            VolumeType = VOLUME_TYPE,
            Size = sz) # In GB, max 16384 for gp2

    # Inherit host's tags
    tags = getInstanceTags()

    if len(tags) > 0:
        ec2client.create_tags(
            Resources=[
                res['VolumeId'],
            ],
            Tags=tags
        )

    return res['VolumeId']

def getEBS_volId(devName):
    iid = getEC2_InstanceId()
    res = ec2client.describe_volumes(
             Filters=[
                  {'Name': 'attachment.instance-id', 'Values': [iid]},
                  {'Name': 'attachment.device', 'Values': [devName]},
             ],
          )
    if len(res['Volumes'])>0:
       return res['Volumes'][0]['VolumeId']

    return None

def getInstanceTags():
    iid = getEC2_InstanceId()
    res = ec2client.describe_tags(
             Filters=[
                   {'Name':'resource-id',
                    'Values':[iid]
                    }]
          )

    savedTags = []
    if len(res['Tags']) > 1:
        for i in res['Tags']:
           if i['Key'] in ['Name', 'aws:ec2spot:fleet-request-id']:
              continue
           savedTags.append({'Key':i['Key'],'Value':i['Value']})
        return savedTags
    else:
        return []



def attachEBS(devName, vol):
    iid = getEC2_InstanceId()

    res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )

    # Check ready Volume
    while res['Volumes'][0]['State'] != 'available':
        time.sleep(1)
        res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )


    res = ec2client.attach_volume(
            InstanceId = iid,
            VolumeId = vol,
            Device = devName,
        )

    time.sleep(1)

    res = ec2client.describe_volumes(
        VolumeIds=[ vol ]
        )

    # wait until attached
    while len(res['Volumes'][0]['Attachments']) < 1:
        time.sleep(1)
        res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )

    ec2client.modify_instance_attribute(
        InstanceId = iid,
        BlockDeviceMappings=[
            {
                'DeviceName': devName,
                'Ebs': {
                    'VolumeId': vol,
                    'DeleteOnTermination': True,
                }
            },
        ]
    )


    if res['Volumes'][0]['Attachments'][0]['State'] == 'attached':
        return 1

    return 0

def detachEBS(devName, vol):
    print ("Detaching {} {}".format(devName,vol))
    iid = getEC2_InstanceId()

    try:
      ec2client.detach_volume(
          Device = devName,
          Force = True,
          InstanceId=iid,
          VolumeId=vol
      )
    except botocore.exceptions.ClientError as e:
      # already detached?
      print("botocore.exceptions.ClientError")
      print(e.__doc__)
      print(e.message)
      return
    except:
      print(e.__doc__)
      print(e.message)
      return

    res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )

    # Check ready Volume
    ct = 0
    while res['Volumes'][0]['State'] != 'available':
        time.sleep(1)
        res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )
        ct += 1
        if ct > 60: break

def deleteEBS(vol):
    print("Deleting " + vol)

    res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )

    # Check ready Volume
    while res['Volumes'][0]['State'] != 'available':
        time.sleep(1)
        print("Waiting to delete when vol is ready")
        res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )

    try:
      ec2client.delete_volume(
          VolumeId = vol
      )
    except Exception as e:
      print(e.__doc__)
      print(e.message)
