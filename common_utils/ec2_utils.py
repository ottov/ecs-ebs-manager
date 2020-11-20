 #!/usr/bin/env python
from __future__ import print_function

import boto3, botocore.exceptions
import time
import requests
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from botocore.config import Config

config = Config(
    retries = dict(
        max_attempts = 6
    )
)

ec2client = boto3.client('ec2',config=config) #,region_name='us-east-1') # Set AWS_DEFAULT_REGION in env

VOLUME_TYPE = 'gp2'
IID = None
ZONE = None

def getEC2_Zone():
    global ZONE
    if ZONE is not None:
        return ZONE
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

    ZONE = r.text
    return r.text

def getEC2_InstanceId():
    global IID
    if IID is not None:
        return IID
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

    IID = r.text
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

    try:
      res = ec2client.create_volume(
            AvailabilityZone = az,
            Encrypted = True,
            VolumeType = VOLUME_TYPE,
            Size = sz) # In GB, max 16384 for gp2

    except botocore.exceptions.ClientError as e:
      logging.exception("Exception")
      print("[createEBS] botocore.exceptions.ClientError")
      print(e.__doc__)
      print(e.message)
      return
    except:
      logging.exception("Caught exception")
      print(e.__doc__)
      print(e.message)
      return


    # Inherit host's tags
    tags = getInstanceTags()

    if len(tags) == 0:
        return res['VolumeId']

    c_ct=0
    c = 0
    while c == 0 and c_ct < 10:
       c = createTags(res['VolumeId'], tags)
       c_ct += 1
       if c_ct > 1:
         print("re-try create_tags")
         time.sleep(1)

    return res['VolumeId']

def createTags(vol, tags):
    try:
        ec2client.create_tags(
            Resources=[
                vol,
            ],
            Tags=tags
        )
    except botocore.exceptions.ClientError as e:
      return 0
    except:
      logging.exception("Caught exception")
      print(e.__doc__)
      print(e.message)
      return 0

    return 1


def getEBS_volId(devName):
    if not devName.startswith('/dev'): return None

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
           if i['Key'].startswith('aws:'):
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

    try:
      res = ec2client.attach_volume(
            InstanceId = iid,
            VolumeId = vol,
            Device = devName,
          )
    except botocore.exceptions.ClientError as e:
      logging.exception("Exception")
      print("[attach] botocore.exceptions.ClientError")
      print(e.__doc__)
      print(e.message)
      return
    except:
      logging.exception("Caught exception")
      print(e.__doc__)
      print(e.message)
      return

    time.sleep(1)

    res = ec2client.describe_volumes(
        VolumeIds=[ vol ]
        )

    # wait until attached
    d_ct = 0
    while len(res['Volumes'][0]['Attachments']) < 1 and d_ct < 18:
        time.sleep(10)
        try:
           res = ec2client.describe_volumes(
               VolumeIds=[ vol ]
               )
        except botocore.exceptions.ClientError as e:
          logging.exception("Exception")
          print("[attach] desc, botocore.exceptions.ClientError")
          print(e.__doc__)
          print(e.message)
          return
        except:
          logging.exception("Caught exception")
          print(e.__doc__)
          print(e.message)
          return

        d_ct += 1
        print("waiting EBS attachment %s: %d" % (vol,d_ct))

    m = 0
    m_ct = 0
    while m == 0 and m_ct<10:
      m = modifyAttr(iid, devName, vol)
      m_ct += 1
      if m_ct > 1:
        time.sleep(1)
        print("re-try modify attr")


    if res['Volumes'][0]['Attachments'][0]['State'] == 'attached':
        return 1

    return 0

def modifyAttr(iid, devName, vol):
    try:
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
    except botocore.exceptions.ClientError as e:
      logging.exception("Exception")
      print("botocore.exceptions.ClientError")
      print(e.__doc__)
      print(e.message)
      return 0
    except:
      logging.exception("Exception")
      print(e.__doc__)
      print(e.message)
      return 0

    return 1


def detachEBS(devName, vol):
    """

    return 1 for success 0 for failure
    """
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
      logging.exception("Exception")
      print("botocore.exceptions.ClientError")
      print(e.__doc__)
      print(e.message)
      return 0
    except:
      logging.exception("Exception")
      print(e.__doc__)
      print(e.message)
      return 0

    res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )

    # Check ready Volume
    ct = 0
    while res['Volumes'][0]['State'] != 'available':
        time.sleep(2)
        print("check detach vol {} in state: {}".format(vol, res['Volumes'][0]['State']))
        res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )
        ct += 1
        if ct > 10: break

    return 1

def deleteEBS(vol):
    print("Deleting " + vol)

    try:
        res = ec2client.describe_volumes(
                VolumeIds=[ vol ]
              )
    except botocore.exceptions.ClientError as e:
      logging.warn("Exception")
      print("botocore.exceptions.ClientError")
      print(e.__doc__)
      print(e.message)
      return
    except:
      logging.exception("Exception")
      print(e.__doc__)
      print(e.message)
      return


    # Check ready Volume
    ct = 0
    while res['Volumes'][0]['State'] != 'available':
        time.sleep(2)
        print("Waiting to delete when vol is ready")
        print("check delete vol {} in state: {}".format(vol, res['Volumes'][0]['State']))
        res = ec2client.describe_volumes(
            VolumeIds=[ vol ]
            )
        ct += 1
        if ct > 10: break

    try:
      ec2client.delete_volume(
          VolumeId = vol
      )
    except Exception as e:
      logging.warn("[deleteEBS] Caught exception")
      print(e.__doc__)
      print(e.message)
      pass
