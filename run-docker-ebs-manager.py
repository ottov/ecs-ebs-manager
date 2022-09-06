#!/usr/bin/env python3

import os
import sys
import time
import docker
import socket
import logging
from subprocess import check_output
from string import ascii_lowercase

from common_utils.ec2_utils import *

logging.basicConfig(filename='/tmp/manager.log', level=logging.ERROR)

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
           cOut = container.exec_run(cmd=['sh','-c', 'grep -m1 -w /scratch /proc/mounts | cut -d" " -f1']).output.decode('ascii')
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


                   d = 0
                   d_ct = 0
                   while d==0 and d_ct < 10:
                      d = detachEBS(devName, vol)
                      d_ct += 1
                      if d_ct > 1:
                         print ("re-try detach")

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
               cOut = container.exec_run(cmd=['sh','-c', 'grep -m1 -w /scratch /proc/mounts | cut -d" " -f1']).output.decode('ascii')
            except docker.errors.APIError:
               print("Caught exception:APIError")
               continue
            except socket.timeout:
               print("Caught exception: socket.timeout")
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
              d = 0
              d_ct = 0
              while d==0 and d_ct < 10:
                 d = detachEBS(devName, vol)
                 d_ct += 1
                 if d_ct > 1:
                    print ("re-try detach")

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
    container.exec_run(cmd='mount -t ext4 %s %s' % (devName, cPath))


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

               sz = str.rstrip(cmd.output.decode('ascii'))             # expecting bytes
               volSz = int(round( (int(float(sz)) + 0.0) / 1024**3) )  # converted to GB for EBS sizing
               volSz += int(volSz * 0.10)                       # +10% !

               # check if /scratch already mounted
               cMounts = c.exec_run('cat /proc/mounts').output.decode('ascii')

               if 'scratch' in cMounts: continue # already mounted

               print('Creating, attaching EBS %s GB' % volSz)
               vol = None
               v_ct = 0
               while vol == None and v_ct < 60:
                  vol = createEBS(volSz)
                  v_ct += 1
                  if v_ct > 1:
                    print("re-try create")
                    time.sleep(2 + v_ct)
                  if v_ct > 57:
                    remove_orphaned_mounts()


               devName = generateDeviceName()
               while devName == None:
                    remove_orphaned_mounts()
                    devName = generateDeviceName()

               res = None
               a_ct = 0
               while res == None and a_ct < 30:
                  res = attachEBS(devName, vol)
                  a_ct += 1
                  if a_ct > 1:
                    print("re-try attach")
                    time.sleep(1 + a_ct)
                  if a_ct > 27:
                    remove_orphaned_mounts()


               print('mounting %s' % devName)
               while not os.path.exists(devName):
                  time.sleep(1)

               check_output('sudo mkfs.ext4 %s' % (devName), shell=True)

               mountEBS_on_container(devName, c.short_id)
               containerMap[cItem] = { 'devname': devName, 'vol': vol }
               time.sleep(2)

            except (docker.errors.NotFound, docker.errors.APIError):
               print("Caught exception: docker not found, must delete.")
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
          logging.exception("Caught exception")
          print("Unexpected error:", sys.exc_info()[0])
          raise

if __name__ == '__main__':
    main()
