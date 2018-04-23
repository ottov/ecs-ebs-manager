#!/usr/bin/env python
from __future__ import print_function

import os
import sys
import time
import docker

from subprocess import check_output
from string import ascii_lowercase

from common_utils.ec2_utils import *

dClient = docker.from_env()

# Global container list
containerMap = dict()

def generateDeviceName():
    """
    Looks for available device names from /dev/xvdcz ... /dev/xvdaa
    """
    for d1 in reversed(ascii_lowercase[0:3]):
        for d2 in reversed(ascii_lowercase):
            try:
                devName = '/dev/xvd%s%s' % (d1,d2)
                f = os.stat(devName)
            except:
                return devName


def buildInventory():
    """
    Modifies global container dict to track any new containers. 
    Helps detect finished tasks
    """
    for container in dClient.containers.list():
        if container.name == 'ecs-agent': continue

        if not container.id in containerMap:
            containerMap[container.id] = None
            cOut = container.exec_run(cmd=['sh','-c', 'df -h /scratch | tail -1 | cut -d" " -f1']).output
            devName = str.rstrip(cOut)
            containerMap[container.id] = { 'devname': devName,
                                           'vol'    : getEBS_volId(devName)
                                         }


def dropFromInventory(cId):
    """
    Removes container from global tracking dict
    """
    if cId in containerMap:
      print ('Removing container %s from inventory' % (cId))
      devName = containerMap[cId]['devname']
      vol     = containerMap[cId]['vol']
      detachEBS(devName, vol)
      deleteEBS(vol)
      del containerMap[cId]
    else:
     print ('Continer %s, not found' %(cId))

def mountEBS_on_container(devName, cId):
    """
    Executes commands on the given privileged container to mount assigned device
    :param devName: block device (e.g. /dev/xvdcz)
    :param cId: container id
    """
    cPath = '/scratch'
    blkStr = check_output('lsblk --noheadings --output MAJ:MIN %s' % (devName), shell=True)


    container = dClient.containers.get(cId)

    container.exec_run(cmd='mknod %s b %s %s ' % (devName, blkStr.split(':')[0], str.rstrip(blkStr.split(':')[1])) )
    container.exec_run(cmd='mkdir -p %s' % (cPath))
    container.exec_run(cmd='mount %s %s' % (devName, cPath))


def main():
    """
    Loops, waiting for containers to appear. 
    Once they are running, checks to see if that have /TOTAL_SIZE request.
    Makes an EBS drive from that request, and mounts it on corresponding container.
    """

    while True:
        buildInventory()

        for cItem in list(containerMap):

            try:
               c = dClient.containers.get(cItem)

               print('Examining %s' % c.name)
               cmd = c.exec_run('cat /TOTAL_SIZE')
               if cmd.exit_code != 0: continue

               sz = str.rstrip(cmd.output) # expecting bytes
               volSz = int(sz) / 1024**3   # converted to GB for EBS sizing
               volSz += 1                  # +1 !

               # check if /scratch already mounted
               cMounts = c.exec_run('cat /proc/mounts').output

               if 'scratch' in cMounts: continue # already mounted

               print('Creating, attaching EBS %s GB' % volSz)
               vol = createEBS(volSz)
               devName = generateDeviceName()
               attachEBS(devName, vol)

               print('mounting %s' % devName)
               check_output('sudo mkfs.ext4 %s' % (devName), shell=True)

               mountEBS_on_container(devName, c.short_id)
               containerMap[cItem] = { 'devname': devName, 'vol': vol }
               time.sleep(3)

            except (docker.errors.NotFound, docker.errors.APIError):
               print("docker not found. must delete.")
               dropFromInventory(cItem)
               continue

        #print('No work. Sleeping')
        try:
          time.sleep(10)
        except KeyboardInterrupt:
          sys.exit(0)
        except:
          print("Unexpected error:", sys.exc_info()[0])
          raise

if __name__ == '__main__':
    main()









