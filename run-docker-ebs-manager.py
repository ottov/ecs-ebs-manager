#!/usr/bin/env python
from __future__ import print_function

import os
import sys
import time
import docker
import socket

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

def remove_orphaned_mounts():
    active_dev_list = set()

    # Gather list of mounted devices inside containers
    for container in dClient.containers.list():
        if container.name == 'ecs-agent': continue

        try:
           cOut = container.exec_run(cmd=['sh','-c', 'grep -m1 -w /scratch /proc/mounts | cut -d" " -f1']).output
        except docker.errors.APIError:
           continue

        devName = str.rstrip(cOut)
        active_dev_list.add(devName)

    # for each possible attachment dev letter, detach/delete EBS if it
    # isn't in active_dev_list
    for d1 in reversed(ascii_lowercase[0:3]):
        for d2 in reversed(ascii_lowercase):
            try:
                devName = '/dev/xvd%s%s' % (d1,d2)
                f = os.stat(devName)
                if devName not in active_dev_list:
                   # found orphan
                   vol = getEBS_volId(devName)
                   if vol == None: continue

                   detachEBS(devName, vol)
                   time.sleep(1)
                   deleteEBS(vol)

            except:
                pass



def buildInventory():
    """
    Modifies global container dict to track any new containers.
    Helps detect finished tasks
    """
    for container in dClient.containers.list():
        if container.name == 'ecs-agent': continue

        if not container.id in containerMap:
            containerMap[container.id] = None

            try:
               cOut = container.exec_run(cmd=['sh','-c', 'grep -m1 -w /scratch /proc/mounts | cut -d" " -f1']).output
            except docker.errors.APIError:
               continue

            devName = str.rstrip(cOut)
            if not 'docker' in devName:
                containerMap[container.id] = { 'devname': devName,
                                               'vol'    : getEBS_volId(devName)
                                              }


def dropFromInventory(cId):
    """
    Removes container from global tracking dict
    :param cId: container id
    """
    if cId in containerMap:
      print ('Removing container %s from inventory' % (cId))
      if containerMap[cId] != None:
           devName = containerMap[cId]['devname']
           vol     = containerMap[cId]['vol']
           if vol != None and 'scratch' not in vol:
              detachEBS(devName, vol)
              time.sleep(1)
              deleteEBS(vol)

      del containerMap[cId]
    else:
     print ('Container %s, not found' %(cId))

def mountEBS_on_container(devName, cId):
    """
    Executes commands on the given privileged container to mount assigned device
    :param devName: block device (e.g. /dev/xvdcz)
    :param cId: container id
    """
    cPath = '/scratch'
    blkStr = check_output('lsblk --noheadings --output MAJ:MIN %s' % (devName), shell=True)


    container = dClient.containers.get(cId)

    print("running root commands on privileged container")
    container.exec_run(cmd='mknod %s b %s %s ' % (devName, blkStr.split(':')[0], str.rstrip(blkStr.split(':')[1])) )
    container.exec_run(cmd='mkdir -p %s' % (cPath))
    container.exec_run(cmd='mount %s %s' % (devName, cPath))


def main():
    """
    Loops, waiting for containers to appear.
    Once they are running, checks to see if it has a file /TOTAL_SIZE request.
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

               sz = str.rstrip(cmd.output)                 # expecting bytes
               volSz = int(round( (int(float(sz)) + 0.0) / 1024**3) )  # converted to GB for EBS sizing
               volSz += int(volSz * 0.10)                       # +10% !

               # check if /scratch already mounted
               cMounts = c.exec_run('cat /proc/mounts').output

               if 'scratch' in cMounts: continue # already mounted

               print('Creating, attaching EBS %s GB' % volSz)
               vol = createEBS(volSz)

               devName = generateDeviceName()
               while devName == None:
                    remove_orphaned_mounts()
                    devName = generateDeviceName()

               attachEBS(devName, vol)

               print('mounting %s' % devName)
               while not os.path.exists(devName):
                  time.sleep(1)

               check_output('sudo mkfs.ext4 %s' % (devName), shell=True)

               mountEBS_on_container(devName, c.short_id)
               containerMap[cItem] = { 'devname': devName, 'vol': vol }
               time.sleep(2)

            except (docker.errors.NotFound, docker.errors.APIError):
               print("docker not found. must delete.")
               dropFromInventory(cItem)
               continue
            except (socket.timeout):
               print("**socket timeout**")
               continue

        #print('No work. Sleeping')
        try:
          time.sleep(5)
        except KeyboardInterrupt:
          sys.exit(0)
        except:
          print("Unexpected error:", sys.exc_info()[0])
          raise

if __name__ == '__main__':
    main()
