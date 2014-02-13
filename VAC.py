#
#  VAC.py - common functions, classes, and variables for Vac
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-4. All rights reserved.
#
#  Redistribution and use in source and binary forms, with or
#  without modification, are permitted provided that the following
#  conditions are met:
#
#    o Redistributions of source code must retain the above
#      copyright notice, this list of conditions and the following
#      disclaimer. 
#    o Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials
#      provided with the distribution. 
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
#  CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
#  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
#  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
#  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
#  TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
#  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
#  POSSIBILITY OF SUCH DAMAGE.
#
#  Contacts: Andrew.McNab@cern.ch  http://www.gridpp.ac.uk/vac/
#

import os
import sys
import uuid
import time
import errno
import base64
import shutil
import libvirt
import datetime
import tempfile
import socket
import stat

from ConfigParser import RawConfigParser

natNetwork     = '169.254.0.0'
natNetmask     = '255.255.0.0'
natPrefix      = '169.254.169.'
factoryAddress = '169.254.169.254'

cycleSeconds = None
deleteOldFiles = None
domainType = None

factories = None
hs06PerMachine = None
mbPerMachine = None

numVirtualmachines = None
spaceName = None
udpTimeoutSeconds = None
vacVersion = None

vcpuPerMachine = None
versionLogger = None
virtualmachines = None
vmtypes = None

volumeGroup = None

def readConf():
      global cycleSeconds, deleteOldFiles, domainType, \
             factories, hs06PerMachine, mbPerMachine, \
             numVirtualmachines, spaceName, udpTimeoutSeconds, vacVersion, \
             vcpuPerMachine, versionLogger, virtualmachines, vmtypes, \
             volumeGroup

      # reset to defaults
      cycleSeconds = 60
      deleteOldFiles = True
      domainType = 'kvm'

      factories = []
      hs06PerMachine = 1.0
      mbPerMachine = 2048

      numVirtualmachines = None
      spaceName = None
      udpTimeoutSeconds = 5.0
      vacVersion = '0.0.0'

      vcpuPerMachine = 1
      versionLogger = True
      virtualmachines = {}
      vmtypes = {}

      volumeGroup = 'vac_volume_group'

      try:
        f = open('/var/lib/vac/doc/VERSION', 'r')
        vacVersion = f.readline().split('=',1)[1].strip()
        f.close()
      except:
        pass
      
      parser = RawConfigParser()

      # Main configuration file, including global [settings] section
      parser.read('/etc/vac.conf')
      
      # Optional file with [factories] listing all factories in this vac space
      parser.read('/etc/vac-factories.conf')

      # Optional file with [targetshares] for all factories in this vac space
      parser.read('/etc/vac-targetshares.conf')

      # general settings from [Settings] section

      if not parser.has_section('settings'):
        return 'Must have a settings section!'
      
      if not parser.has_option('settings', 'vac_space'):
        return 'Must give vac_space in [settings]!'
        
      spaceName = parser.get('settings','vac_space').strip()
             
      if parser.has_option('settings', 'domain_type'):
          # defaults to 'kvm' but can specify 'xen' instead
          domainType = parser.get('settings','domain_type').strip()

      if parser.has_option('settings', 'total_machines'):
          # Mandatory number of VMs for Vac to auto-define.
          # No longer use [virtualmachine ...] sections!
          numVirtualmachines = int(parser.get('settings','total_machines').strip())
      else:
          return 'Must give total_machines in [settings]!'
                                                 
      if parser.has_option('settings', 'volume_group'):
          # Volume group to search for logical volumes 
          volumeGroup = parser.get('settings','volume_group').strip()
             
      if parser.has_option('settings', 'cycle_seconds'):
          # How long to wait before re-evaluating state of VMs in the
          # main loop again. Defaults to 60 seconds.
          cycleSeconds = int(parser.get('settings','cycle_seconds').strip())

      if parser.has_option('settings', 'udp_timeout_seconds'):
          # How long to wait before giving up on more UDP replies          
          udpTimeoutSeconds = float(parser.get('settings','udp_timeout_seconds').strip())

      if (parser.has_option('settings', 'version_logger') and
          parser.get('settings','version_logger').strip().lower() == 'false'):
           versionLogger = False
      else:
           versionLogger = True

      if (parser.has_option('settings', 'delete_old_files') and
          parser.get('settings','delete_old_files').strip().lower() == 'false'):
           deleteOldFiles = False
      else:
           deleteOldFiles = True
             
      if parser.has_option('settings', 'vcpu_per_machine'):
          # if this isn't set, then we allocate one vcpu per VM
          vcpuPerMachine = int(parser.get('settings','vcpu_per_machine'))
             
      if parser.has_option('settings', 'mb_per_machine'):
          # if this isn't set, then we use default (2048 MiB)
          mbPerMachine = int(parser.get('settings','mb_per_machine'))

      if parser.has_option('settings', 'hs06_per_machine'):
          # if this isn't set, then we keep default (1.0)
          hs06PerMachine = float(parser.get('settings','hs06_per_machine'))
             
      # all other sections are VM types or Virtual Machines or Factories
      for sectionName in parser.sections():

         if (sectionName.lower() == 'settings'):
           continue 
           
         sectionNameSplit = sectionName.lower().split(None,1)
         
         if sectionNameSplit[0] == 'vmtype':
             vmtype = {}
             vmtype['root_image'] = parser.get(sectionName, 'root_image')

             vmtype['share'] = 0.0
                                            
             if parser.has_option('targetshares', sectionNameSplit[1]):
                 vmtype['share'] = float(parser.get('targetshares', sectionNameSplit[1]))

             if parser.has_option(sectionName, 'root_device'):
                 vmtype['root_device'] = parser.get(sectionName, 'root_device')
             else:
                 vmtype['root_device'] = 'hda'
             
             if parser.has_option(sectionName, 'scratch_device'):
                 vmtype['scratch_device'] = parser.get(sectionName, 'scratch_device')
             else:
                 vmtype['scratch_device'] = 'hdb'

             if parser.has_option(sectionName, 'rootpublickey'):
                 vmtype['rootpublickey'] = parser.get(sectionName, 'rootpublickey')

             if parser.has_option(sectionName, 'user_data'):
                 vmtype['user_data'] = parser.get(sectionName, 'user_data')

             if parser.has_option(sectionName, 'prolog'):
                 vmtype['prolog'] = parser.get(sectionName, 'prolog')

             if parser.has_option(sectionName, 'epilog'):
                 vmtype['epilog'] = parser.get(sectionName, 'epilog')

             if parser.has_option(sectionName, 'log_machineoutputs') and \
                parser.get(sectionName,'log_machineoutputs').strip().lower() == 'true':
                 vmtype['log_machineoutputs'] = True
             else:
                 vmtype['log_machineoutputs'] = False
             
             if parser.has_option(sectionName, 'max_wallclock_seconds'):
                 vmtype['max_wallclock_seconds'] = int(parser.get(sectionName, 'max_wallclock_seconds'))
             else:
                 vmtype['max_wallclock_seconds'] = 86400
             
             if parser.has_option(sectionName, 'shutdown_command'):
                 vmtype['shutdown_command'] = parser.get(sectionName, 'shutdown_command')

             if parser.has_option(sectionName, 'backoff_seconds'):
                 vmtype['backoff_seconds'] = int(parser.get(sectionName, 'backoff_seconds'))
             else:
                 vmtype['backoff_seconds'] = 10
             
             if parser.has_option(sectionName, 'fizzle_seconds'):
                 vmtype['fizzle_seconds'] = int(parser.get(sectionName, 'fizzle_seconds'))
             else:
                 vmtype['fizzle_seconds'] = 600
            
             if parser.has_option(sectionName, 'accounting_fqan'):
                 vmtype['accounting_fqan'] = parser.get(sectionName, 'accounting_fqan')
                          
             vmtypes[sectionNameSplit[1]] = vmtype
             
         elif sectionName.lower() == 'factories':
             try:
                 factories = (parser.get('factories', 'names')).lower().split()
             except:
                 pass
                          
      # Define VMs
      ordinal = 0
         
      while ordinal < numVirtualmachines:           
           virtualmachine = {}
           
           virtualmachine['ordinal'] = ordinal
           
           nameParts = os.uname()[1].split('.',1)
           
           vmName = nameParts[0] + '-%02d' % ordinal + '.' + nameParts[1]
                      
           if os.path.exists('/dev/' + volumeGroup + '/' + vmName) and \
              stat.S_ISBLK(os.stat('/dev/' + volumeGroup + '/' + vmName).st_mode):
                virtualmachine['scratch_volume'] = '/dev/' + volumeGroup + '/' + vmName
           
           virtualmachines[vmName] = virtualmachine
           ordinal += 1

      # Finished successfully, with no error to return
      return None
        
class VacState:
   unknown, shutdown, starting, running, paused, zombie = ('Unknown', 'Shut down', 'Starting', 'Running', 'Paused', 'Zombie')

class VacVM:
   def __init__(self, hname):
      self.name=hname
      self.state=VacState.unknown
      self.uuidStr=None
      self.vmtypeName=None
      self.finishedFile=None
      self.cpuSeconds = 0

      conn = libvirt.open(None)
      if conn == None:
          print 'Failed to open connection to the hypervisor'
          raise

      try:
          dom = conn.lookupByName(self.name)          
          self.uuidStr = dom.UUIDString()

          try:
            self.cpuSeconds = int(dom.info()[4] / 1000000000.0)
          except:
            pass

          for vmtypeName, vmtype in vmtypes.iteritems():
               if os.path.isdir('/var/lib/vac/machines/' + self.name + '/' + vmtypeName + '/' + self.uuidStr):
                   self.vmtypeName = vmtypeName
                   break
                  
          domState = dom.info()[0]
          
          if not self.vmtypeName:
            self.state = VacState.zombie
            self.uuidStr = None
          elif (domState == libvirt.VIR_DOMAIN_RUNNING or
                (domState == libvirt.VIR_DOMAIN_NOSTATE and domainType == 'xen') or
                domState == libvirt.VIR_DOMAIN_BLOCKED):
            self.state = VacState.running
          else:
            self.state = VacState.paused
            logLine('!!! libvirt state is ' + str(domState) + ', setting VacState.paused !!!')

      except:
          self.state = VacState.shutdown
 
          # try to find state of last instance to be created
          self.uuidFromLatestVM()

          try:
            f = open('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/heartbeat', 'r')
            self.cpuSeconds = int(f.readline().strip())
            f.close()
          except:
            pass

          if self.uuidStr and self.vmtypeName \
             and not os.path.exists('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + 
                                                                    '/' + self.uuidStr + '/started'):
              self.state = VacState.starting
                        
          try: 
            f = open('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr
                                         + '/shared/machineoutputs/shutdown_message', 'r')
            self.shutdownMessage = f.readline().strip()
            f.close()

            self.timeShutdownMessage = int(os.stat('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + 
                                            '/' + self.uuidStr + '/shared/machineoutputs/shutdown_message').st_ctime)
          except:
            self.shutdownMessage = None
            self.timeShutdownMessage = None
            pass

      conn.close()
      
      try:
           self.started = int(os.stat('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + 
                                            '/' + self.uuidStr + '/started').st_ctime)
      except:
           self.started = None
                          
      try:
           self.heartbeat = int(os.stat('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + 
                                            '/' + self.uuidStr + '/heartbeat').st_ctime)
      except:
           self.heartbeat = None

      if self.uuidStr and os.path.exists('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr 
                                         + '/shared/machinefeatures/shutdowntime') :
          f = open('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr 
                                         + '/shared/machinefeatures/shutdowntime')
          self.shutdownTime = int(f.read().strip())
          f.close()
      
      if self.uuidStr and os.path.exists('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr 
                                         + '/finished') :
          self.finishedFile = True
      else:
          self.finishedFile = False

   def createHeartbeatFile(self):
      self.heartbeat = int(time.time())
      try:
        createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' +
                            self.uuidStr + '/heartbeat', str(self.cpuSeconds) + '\n')
      except:
        pass
                                  
   def createFinishedFile(self):
      try:
        f = open('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/finished', 'w')
        f.close()
      except:
        logLine('Failed creating /var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/finished')

   def writeAccounting(self):
      # Write accounting information about a VM that has finished
      if self.state != VacState.shutdown or not self.started or not self.heartbeat:
        return

      # If the VM just ran for fizzle_seconds, then we don't log it
      if (self.heartbeat - self.started) < vmtypes[self.vmtypeName]['fizzle_seconds']:
        return

      # Just in case it's been cleaned away somehow
      try:
        os.makedirs('/var/log/vacd-accounting')
      except:
        pass

      # PBS/Torque accounting file (uses localtime)
      try:
        pbsFile = open(time.strftime('/var/log/vacd-accounting/%Y%m%d', time.localtime()), 'a+')
      except:
        logLine('Failed opening ' + time.strftime('/var/log/vacd-accounting/%Y%m%d', time.localtime()))
        return
      
      # BLAHP accounting file (uses UTC/GMT!)
      try:
        blahpFile = open(time.strftime('/var/log/vacd-accounting/blahp.log-%Y%m%d', time.gmtime()), 'a+')
      except:
        logLine('Failed opening ' + time.strftime('/var/log/vacd-accounting/blahp.log-%Y%m%d', time.gmtime()))
        pbsFile.close()
        return
      
      pbsFile.write(time.strftime('%m/%d/%Y %H:%M:%S;E;', time.localtime()) + 
              self.uuidStr + ';user=' + self.vmtypeName +
              ' group=' + self.vmtypeName + 
              ' jobname=' + self.uuidStr + 
              ' queue=' + self.vmtypeName + 
              ' ctime=' + str(self.started) +
              ' qtime=' + str(self.started) +
              ' etime=' + str(self.started) +
              ' start=' + str(self.started) +
              ' owner=' + self.vmtypeName + '@' + spaceName + 
              ' exec_host=' + os.uname()[1] + '/' + str(virtualmachines[self.name]['ordinal']) + 
              ' Resource_List.cput=' + secondsToHHMMSS(vmtypes[self.vmtypeName]['max_wallclock_seconds']) +
              ' Resource_List.neednodes=1 Resource_List.nodect=1 Resource_List.nodes=1' +
              ' Resource_List.walltime=' + secondsToHHMMSS(vmtypes[self.vmtypeName]['max_wallclock_seconds']) +
              ' session=0' +
              ' end=' + str(self.heartbeat) + 
              ' Exit_status=0' +
              ' resources_used.cput=' + secondsToHHMMSS(self.cpuSeconds) + 
              ' resources_used.mem=' + str(mbPerMachine * 1024) + 'kb resources_used.vmem=' + str(mbPerMachine * 1024) + 'kb' +
              ' resources_used.walltime=' + secondsToHHMMSS(self.heartbeat - self.started) + '\n')
                          
      pbsFile.close()

      userDN = ''
      for component in spaceName.split('.'):
        userDN = '/DC=' + component + userDN
        
      if 'accounting_fqan' in vmtypes[self.vmtypeName]:
        userFQANField = '"userFQAN=' + vmtypes[self.vmtypeName]['accounting_fqan'] + '" '
      else:
        userFQANField = ''

      blahpFile.write(time.strftime('"timestamp=%Y-%m-%d %H:%M:%S" ', time.gmtime()) + 
              '"userDN=' + userDN + '" ' + userFQANField +
              '"ceID=' + spaceName + '" ' +
              '"jobID=' + self.uuidStr + '" ' +
              '"lrmsID=' + self.uuidStr + '" ' +
              '"localUser=99" ' +
              '"clientID=' + self.uuidStr + '"\n')
                           
      blahpFile.close()

   def logMachineoutputs(self):
   
      try:
        # Get the list of files that the VM has left in its /etc/machineoutputs
        outputs = os.listdir('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machineoutputs')
      except:
        logLine('Failed reading /var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machineoutputs')
        return
        
      try:
        f = open('/var/log/vacd-machineoutputs', 'a+')
      except:
        logLine('Failed opening /var/log/vacd-machineoutputs')
        return
      
      if not outputs:
        # Nothing to do if there are no files there
        f.write('=====' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + '===== ' + 
                self.uuidStr + ' ' + self.name + ' ' + self.vmtypeName + ' has no outputs =====\n')
      else:
        f.write('\n')
        # Go through the files one by one, appending them to /var/log/vacd-machineoutputs
        for oneOutput in outputs:
        
          contents = None
          try:
             g = open('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machineoutputs/' + oneOutput, 'r')
             contents = g.read()
             g.close()
          except:
             pass

          if contents:
             f.write('====='  + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + '===== ' + 
                     self.uuidStr + ' ' + self.name + ' ' + self.vmtypeName + ' /etc/machineoutputs/' + oneOutput + ' ==start===\n')
             f.write(contents)
             f.write('====='  + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + '===== ' + 
                     self.uuidStr + ' ' + self.name + ' ' + self.vmtypeName + ' /etc/machineoutputs/' + oneOutput + ' ==finish==\n')
          else:
             f.write('====='  + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + '===== ' + 
                     self.uuidStr + ' ' + self.name + ' ' + self.vmtypeName + ' /etc/machineoutputs/' + oneOutput + ' is empty =====\n')
                        
      f.close()
   
   def uuidFromLatestVM(self):

      self.uuidStr    = None
      self.vmtypeName = None
      
      for vmtypeName, vmtype in vmtypes.iteritems():
        try:
             dirslist = os.listdir('/var/lib/vac/machines/' + self.name + '/' + vmtypeName)
        except:
             continue

        for onedir in dirslist:
          if os.path.isdir('/var/lib/vac/machines/' + self.name + '/' + vmtypeName + '/' + onedir):
             if self.vmtypeName and self.uuidStr:
                if os.stat('/var/lib/vac/machines/' + self.name + '/' + vmtypeName + '/' + onedir).st_ctime > os.stat('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr).st_ctime:
                  self.uuidStr = onedir          
                  self.vmtypeName = vmtypeName
             else:
               self.vmtypeName = vmtypeName
               self.uuidStr = onedir
          
   def makeISO(self):
      try:
        os.makedirs('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/iso.d')
      except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/iso.d'):
            pass 
        else: raise

      f = open('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/iso.d/context.sh', 'w')

      if 'rootpublickey' in vmtypes[self.vmtypeName]:

          if vmtypes[self.vmtypeName]['rootpublickey'][0] == '/':
              rootpublickey_file = vmtypes[self.vmtypeName]['rootpublickey']
          else:
              rootpublickey_file = '/var/lib/vac/vmtypes/' + self.vmtypeName + '/' + vmtypes[self.vmtypeName]['rootpublickey']

          shutil.copy2(rootpublickey_file, '/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/iso.d/root.pub')
          f.write('ROOT_PUBKEY=root.pub\n')
  
      if 'user_data' in vmtypes[self.vmtypeName]:

          if vmtypes[self.vmtypeName]['user_data'][0] == '/':
              user_data_file = vmtypes[self.vmtypeName]['user_data']
          else:
              user_data_file = '/var/lib/vac/vmtypes/' + self.vmtypeName + '/' + vmtypes[self.vmtypeName]['user_data']

          try:
            u = open(user_data_file, 'r')
          except:
            raise 'Failed to open' + user_data_file  
            
          user_data_contents = u.read()
          u.close()
          f.write('EC2_USER_DATA=' +  base64.b64encode(user_data_contents) + '\n')
  
      f.write('ONE_CONTEXT_PATH="/var/lib/amiconfig"\n')
      f.write('MACHINEFEATURES="/etc/machinefeatures"\n')
      f.write('JOBFEATURES="/etc/jobfeatures"\n')
      f.close()
                                     
      f = open('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/iso.d/prolog.sh', 'w')

      f.write('#!/bin/sh\n')
      f.write('if [ "$1" = "start" ] ; then\n')
      f.write('  hostname ' + self.name + '\n')
      f.write('  mkdir -p /etc/machinefeatures /etc/jobfeatures /etc/machineoutputs /etc/vmtypefiles\n')
      f.write('  cat <<EOF >/etc/cernvm/cernvm.d/S50vac.sh\n')
      f.write('#!/bin/sh\n')
      f.write('mount ' + factoryAddress + ':/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures /etc/jobfeatures\n')
      f.write('mount ' + factoryAddress + ':/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures /etc/machinefeatures\n')
      f.write('mount -o rw ' + factoryAddress + ':/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machineoutputs /etc/machineoutputs\n')

      if os.path.isdir('/var/lib/vac/vmtypes/' + self.vmtypeName + '/shared'):
        f.write('mount ' + factoryAddress + ':/var/lib/vac/vmtypes/' + self.vmtypeName + '/shared /etc/vmtypefiles\n')

      f.write('EOF\n')
      f.write('  chmod ugo+x /etc/cernvm/cernvm.d/S50vac.sh\n')
      f.write('fi\n# end of vac prolog.sh\n\n')

      # if a prolog is given for this vmtype, we append that to vac's part of the script
      if 'prolog' in vmtypes[self.vmtypeName]:

          if vmtypes[self.vmtypeName]['prolog'][0] == '/':
              prolog_file = vmtypes[self.vmtypeName]['prolog']
          else:
              prolog_file = '/var/lib/vac/vmtypes/' + self.vmtypeName + '/' + vmtypes[self.vmtypeName]['prolog']

          try:
            g = open(prolog_file, "r")
            f.write(g.read())
            g.close()
          except:
            raise 'Failed to read prolog file', prolog_file
  
      f.close()
      
      # we include any specified epilog in the CD-ROM image without modification
      if 'epilog' in vmtypes[self.vmtypeName]:

          if vmtypes[self.vmtypeName]['epilog'][0] == '/':
              epilog_file = vmtypes[self.vmtypeName]['epilog']
          else:
              epilog_file = '/var/lib/vac/vmtypes/' + self.vmtypeName + '/' + vmtypes[self.vmtypeName]['epilog']

          shutil.copy2(epilog_file, '/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/iso.d/epilog.sh')
  
      os.system('genisoimage -quiet -o /var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr 
                + '/context.iso /var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/iso.d')
             
   def exportFileSystems(self):
      os.makedirs('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures')
      os.makedirs('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machineoutputs')
      os.makedirs('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures')

      # Vac specific extensions
             
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/vac_factory',
                 os.uname()[1] + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/vac_vmtype',
                 self.vmtypeName + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/vac_space',
                 spaceName + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/vac_uuid',
                 self.uuidStr + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)
    
      # Standard machinefeatures

      # HEPSPEC06 per virtual machine
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/hs06',
                 str(hs06PerMachine) + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # we don't know the physical vs logical cores distinction here so we just use vcpu
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/phys_cores',
                 str(vcpuPerMachine) + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # again just use vcpu
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/log_cores',
                 str(vcpuPerMachine) + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # tell them they have the whole VM to themselves; they are in the only jobslot here
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/jobslots',
                '1\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)
      
      if 'shutdown_command' in vmtypes[self.vmtypeName]:
        createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/shutdown_command',
                   vmtypes[self.vmtypeName]['shutdown_command'] + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # calculate the absolute shutdown time for the VM, as a machine
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machinefeatures/shutdowntime',
                 str(int(time.time() + vmtypes[self.vmtypeName]['max_wallclock_seconds']))  + '\n', 
                 mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # Standard  jobfeatures
      
      # calculate the absolute shutdown time for the VM, as a job
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/shutdowntime_job',
                 str(int(time.time() + vmtypes[self.vmtypeName]['max_wallclock_seconds']))  + '\n', 
                 mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # we don't do this, so just say 1.0 for cpu factor
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/cpufactor_lrms',
                 '1.0\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # for the scaled and unscaled cpu limit, we use the wallclock seconds multiple by the vcpu            
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/cpu_limit_secs_lrms',
                 str(vmtypes[self.vmtypeName]['max_wallclock_seconds']) * vcpuPerMachine + '\n', 
                 mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/cpu_limit_secs',
                 str(vmtypes[self.vmtypeName]['max_wallclock_seconds']) * vcpuPerMachine + '\n', 
                 mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # for the scaled and unscaled wallclock limit, we use the wallclock seconds without factoring in vcpu
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/cpu_limit_secs_lrms',
                 str(vmtypes[self.vmtypeName]['max_wallclock_seconds']) + '\n', 
                 mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/cpu_limit_secs',
                 str(vmtypes[self.vmtypeName]['max_wallclock_seconds']) + '\n', 
                 mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # if we know the size of the scratch partition, we use it as the disk_limit_GB (1000^3 not 1024^3 bytes)
      if virtualmachines[self.name]['scratch_volume_gb']:
         createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/disk_limit_GB',
                 str(virtualmachines[self.name]['scratch_volume_gb']) + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # we are about to start the VM now
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/jobstart_secs',
                 str(int(time.time())) + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # mbPerMachine is in units of 1024^2 bytes, whereas jobfeatures wants 1000^2
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/mem_limit_MB',
                 str(int(mbPerMachine * 1.048576)) + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)
                        
      # vcpuPerMachine again
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/jobfeatures/allocated_CPU',
                 str(vcpuPerMachine) + '\n', mode=stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

      # do the NFS exports

      exportAddress = natPrefix + str(virtualmachines[self.name]['ordinal'])

      if os.path.exists('/var/lib/vac/vmtypes/' + self.vmtypeName + '/shared'):
         os.system('exportfs ' + exportAddress + ':/var/lib/vac/vmtypes/' + self.vmtypeName + '/shared')

      os.system('exportfs ' + exportAddress + ':/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared')
      os.system('exportfs -o rw,no_root_squash ' + exportAddress + ':/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/shared/machineoutputs')

   def makeRootDisk(self):
      if domainType == 'kvm':
         # With kvm we can make a small QEMU qcow2 disk for each instance of 
         # this virtualhostname, backed by the full image given in conf
         if os.system('qemu-img create -b ' + vmtypes[self.vmtypeName]['root_image'] + 
             ' -f qcow2 /var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/root.disk >/dev/null') != 0:
          logLine('creation of COW disk image fails!')
          raise NameError('Creation of COW disk image fails!')
      elif domainType == 'xen':
         # Because Xen COW is broken, we copy the root.disk, overwriting 
         # any copy already in the top level directory of this virtualhostname.
         # To avoid long startups, the source should be a sparse file too.
         logLine('copy from ' + vmtypes[self.vmtypeName]['root_image'] + ' to /var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/root.disk')
         if os.system('/bin/cp --force --sparse=always ' + vmtypes[self.vmtypeName]['root_image'] +
                       ' /var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/root.disk') != 0:
          logLine('copy of disk image fails!')
          raise NameError('copy of disk image fails!')

   def makeScratchDisk(self):
      os.system('mkfs -q -t ext3 ' + virtualmachines[self.name]['scratch_volume'])      

      try:
       # get logical volume size in GB (1000^3 not 1024^3)
       f = os.popen('lvs --nosuffix --units G --noheadings -o lv_size ' + virtualmachines[self.name]['scratch_volume'] + ' 2>/dev/null', 'r')
       sizeGB = float(f.readline())
       f.close()
       virtualmachines[self.name]['scratch_volume_gb'] = sizeGB
      except:
       logLine('failed to read size of ' + virtualmachines[self.name]['scratch_volume'] + ' using lvs command')
       pass      

   def destroyVM(self):
      conn = libvirt.open(None)
      if conn == None:
          logLine('Failed to open connection to the hypervisor')
          raise NameError('failed to open connection to the hypervisor')

      dom = conn.lookupByName(self.name)
      
      if dom:
          dom.destroy()
          self.state = VacState.shutdown

      conn.close()

   def createVM(self, vmtypeName):
      self.uuidStr = str(uuid.uuid4())
      self.vmtypeName = vmtypeName

      os.makedirs('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr)

      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/created', 
                  str(int(time.time())) + '\n')

      self.makeISO()
      self.makeRootDisk()

      if 'scratch_volume' in virtualmachines[self.name]:
          self.makeScratchDisk()
          if domainType == 'kvm':
            scratch_volume_xml = ("<disk type='block' device='disk'>\n" +
                                  " <driver name='qemu' type='raw'/>\n" +
                 " <source dev='" + virtualmachines[self.name]['scratch_volume']  + "'/>\n" +
                                  " <target dev='" + vmtypes[self.vmtypeName]['scratch_device'] + "' bus='ide'/>\n</disk>")
          elif domainType == 'xen':
            scratch_volume_xml = ("<disk type='block' device='disk'>\n" +
                                  " <driver name='phy'/>\n" +
                 " <source dev='" + virtualmachines[self.name]['scratch_volume']  + "'/>\n" +
                                  " <target dev='" + vmtypes[self.vmtypeName]['scratch_device'] + "' bus='ide'/>\n</disk>")
      else:
          scratch_volume_xml = ""

      ip = natPrefix + str(virtualmachines[self.name]['ordinal'])

      ipBytes = ip.split('.')
        
      mac = '56:4D:%02X:%02X:%02X:%02X' % (int(ipBytes[0]), int(ipBytes[1]), int(ipBytes[2]), int(ipBytes[3]))
                   
      logLine('Using MAC ' + mac + ' when creating ' + self.name)

      # this goes after the rest of the setup since it populates machinefeatures and jobfeatures
      self.exportFileSystems()

      try:
          conn = libvirt.open(None)
      except:
          return 'exception when opening connection to the hypervisor'

      if conn == None:
          return 'failed to open connection to the hypervisor'
                
      if domainType == 'kvm':
          xmldesc="""<domain type='kvm'>
  <name>""" + self.name + """</name>
  <uuid>""" + self.uuidStr + """</uuid>
  <memory unit='MiB'>""" + str(mbPerMachine) + """</memory>
  <currentMemory unit='MiB'>"""  + str(mbPerMachine) + """</currentMemory>
  <vcpu>""" + str(vcpuPerMachine) + """</vcpu>
  <os>
    <type arch='x86_64' machine='rhel6.2.0'>hvm</type>
    <boot dev='network'/>
    <bios useserial='yes'/>
  </os>
  <pm>
    <suspend-to-disk enabled='no'/>
    <suspend-to-mem  enabled='no'/>
  </pm>
  <features>
    <acpi/>
    <apic/>
    <pae/>
  </features>
  <clock offset='utc'/>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>destroy</on_reboot>
  <on_crash>destroy</on_crash>
  <devices>
    <emulator>/usr/libexec/qemu-kvm</emulator>
    <disk type='file' device='disk'>
     <driver name="qemu" type="qcow2" cache="none" />
     <source file='/var/lib/vac/machines/""" + self.name + '/' + self.vmtypeName + '/' + self.uuidStr +  """/root.disk' />
     <target dev='""" + vmtypes[self.vmtypeName]['root_device'] + """' bus='ide'/>
    </disk>""" + scratch_volume_xml + """
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='/var/lib/vac/machines/""" + self.name + '/' + self.vmtypeName + '/' + self.uuidStr +  """/context.iso'/>
      <target dev='hdd'/>
      <readonly/>
    </disk>
    <controller type='usb' index='0'>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x2'/>
    </controller>
    <interface type='network'>
      <mac address='""" + mac + """'/>
      <source network='vac_""" + natNetwork + """"'/>
      <model type='virtio'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x03' function='0x0'/>
    </interface>
    <serial type='pty'>
      <target port='0'/>
    </serial>
    <graphics type='vnc' port='-1' autoport='yes' keymap='en-gb'/>
    <video>
      <model type='vga' vram='9216' heads='1'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x02' function='0x0'/>
    </video>
    <memballoon model='virtio'>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x05' function='0x0'/>
    </memballoon>
  </devices>
</domain>
"""        
      elif domainType == 'xen':
          xmldesc="""<domain type='xen'>
  <name>""" + self.name + """</name>
  <uuid>""" + self.uuidStr + """</uuid>
  <memory unit='MiB'>""" + str(mbPerMachine) + """</memory>
  <currentMemory unit='MiB'>""" + str(mbPerMachine) + """</currentMemory>
  <vcpu>""" + str(vcpuPerMachine) + """</vcpu>
  <bootloader>/usr/bin/pygrub</bootloader>
  <os>
    <type arch='x86_64' machine='xenpv'>linux</type>
  </os>
  <clock offset='utc' adjustment='reset'/>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>restart</on_reboot>
  <on_crash>restart</on_crash>
  <devices>
    <disk type='file' device='disk'>
      <driver name='file'/>
      <source file='/var/lib/vac/machines/""" + self.name + '/' + self.vmtypeName + '/' + self.uuidStr +  """/root.disk' />
      <target dev='""" + vmtypes[self.vmtypeName]['root_device'] + """' bus='ide'/>
    </disk>""" + scratch_volume_xml + """
    <disk type='file' device='cdrom'>
      <driver name='file'/>
      <source file='/var/lib/vac/machines/""" + self.name + '/' + self.vmtypeName + '/' + self.uuidStr +  """/context.iso'/>
      <target dev='hdd'/>
      <readonly/>
    </disk>
    <console type='pty'>
      <target type='xen' port='0'/>
    </console>
    <graphics type='vnc' port='-1' autoport='yes' listen='0.0.0.0' keymap='en-us'>
      <listen type='address' address='0.0.0.0'/>
    </graphics>
    <interface type='network'>
      <mac address='""" + mac + """'/>
      <source network='vac_""" + natNetwork + """'/>
      <model type='virtio'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x03' function='0x0'/>
    </interface>
  </devices>
</domain>
"""        

      else:
          conn.close()
          return 'domain_type not recognised!'
      
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/started', 
                  str(int(time.time())) + '\n')
      
      createFile('/var/lib/vac/machines/' + self.name + '/' + self.vmtypeName + '/' + self.uuidStr + '/heartbeat', 
                 '0.0\n')
      
      try:
           dom = conn.createXML(xmldesc, 0)           
      except:
           logLine('Exception when trying to create VM domain for ' + self.name)
           conn.close()
           return 'exception when trying to create VM domain'
           
      if not dom:
           logLine('Failed when trying to create VM domain for ' + self.name)
           conn.close()
           return 'failed when trying to create VM domain'
           
      conn.close()
       
      self.state = VacState.running

      # Everything ok so return no error message
      return None

def createNetwork(conn):

      nameParts = os.uname()[1].split('.',1)

      dhcpXML = ""
 
      ordinal = 0
      while ordinal < 100:
    
        ip      = natPrefix + str(ordinal)
        ipBytes = ip.split('.')        
        mac     = '56:4D:%02X:%02X:%02X:%02X' % (int(ipBytes[0]), int(ipBytes[1]), int(ipBytes[2]), int(ipBytes[3]))
        vmName  = nameParts[0] + '-%02d' % ordinal + '.' + nameParts[1]
        hostsLine = ip + ' ' + nameParts[0] + '-%02d' % ordinal + ' ' + vmName + ' # added by Vac'

        dhcpXML += "   <host mac='" + mac + "' name='" + vmName + "' ip='" + ip + "'/>\n"
        ordinal += 1

        # append a line for this VM to /etc/hosts if not already present
        with open('/etc/hosts', 'r') as f:
          if not hostsLine in f.read():
           f.close()
           with open('/etc/hosts', 'a') as g:
            g.write(hostsLine + '\n')

      netXML = "<network>\n <name>vac_" + natNetwork + "</name>\n <forward mode='nat'/>\n"
      netXML += " <ip address='" + factoryAddress + "' netmask='" + natNetmask + "'>\n"
      netXML += "  <dhcp>\n" + dhcpXML + "</dhcp>\n </ip>\n</network>\n"
      
      try:
        if conn.networkCreateXML(netXML):
         logLine('Network vac_' + natNetwork + ' created.')
         return True
        else:
         logLine('Failed to create NAT network vac_' + natNetwork)
         return False
      except:
        logLine('Failed to create NAT network vac_' + natNetwork + ' (Need dnsmasq RPM >= 2.48-13?)')
        return False

      # we never get here...
      return False     
     
def createFile(targetname, contents, mode=None):
      # Create a text file containing contents in the vac tmp directory
      # then move it into place. Rename is an atomic operation in POSIX,
      # including situations where targetname already exists.
   
      try:
       ftup = tempfile.mkstemp(prefix='/var/lib/vac/tmp',text=True)
       os.write(ftup[0], contents)
       
       if mode: 
         os.fchmod(ftup[0], mode)

       os.close(ftup[0])
       os.rename(ftup[1], targetname)
       return True
      except:
       return False

def logLine(text):
      print time.strftime('%b %d %H:%M:%S [') + str(os.getpid()) + ']: ' + text
      sys.stdout.flush()

def secondsToHHMMSS(seconds):
      hh, ss = divmod(seconds, 3600)
      mm, ss = divmod(ss, 60)
      return '%02d:%02d:%02d' % (hh, mm, ss)
      
def cleanupByNameUUID(name, vmtypeName, uuidStr):
   conn = libvirt.open(None)
   if conn == None:
      print 'Failed to open connection to the hypervisor'
      raise
          
   try:
      dom = conn.lookupByUUIDString(uuidStr)
      dom.destroy()
   except:
      pass

   f = os.popen('exportfs', 'r')
   pathname = f.readline().strip()

   while pathname and name:
      if ('/var/lib/vac/machines/' + name + '/' + vmtypeName + '/' + uuidStr + '/shared' == pathname):
         os.system('exportfs -u ' + name + ':' + pathname)

      pathname = f.readline().strip()

   f.close()

   shutil.rmtree('/var/lib/vac/machines/' + name + '/' + vmtypeName + '/' + uuidStr, 1)
   
def cleanupExports():

   conn = libvirt.open(None)
   if conn == None:
        print 'Failed to open connection to the hypervisor'
        raise

   f = os.popen('exportfs', 'r')
   exportPath = f.readline().strip()
   exportHost = f.readline().strip()
   pathsplit  = exportPath.split('/')
   
   while exportPath and exportHost:

      #  /var/lib/vac/machines/f.q.d.n/vmtype/UUID/shared
      # 0  1   2   3      4       5      6     7     8

      if (len(pathsplit) > 8) and pathsplit[0] == '' and pathsplit[1] == 'var' and \
         pathsplit[2] == 'lib' and pathsplit[3] == 'vac' and pathsplit[4] == 'machines' and \
         pathsplit[8] == 'shared':
     
            try:
              dom = conn.lookupByUUIDString(pathsplit[7])

            except: 
              print 'Remove now unused export of', exportPath
              os.system('exportfs -u ' + exportHost + ':' + exportPath)
    
      #  /var/lib/vac/vmtypes/vmtype/shared
      # 0  1   2   3     4      5      6 

      elif (len(pathsplit) > 6) and pathsplit[0] == '' and pathsplit[1] == 'var' and \
         pathsplit[2] == 'lib' and pathsplit[3] == 'vac' and pathsplit[4] == 'vmtypes' and \
         pathsplit[6] == 'shared' and (pathsplit[5] not in vmtypes):
         
              print 'Remove now unused export of', exportPath
              os.system('exportfs -u ' + exportHost + ':' + exportPath)              

      exportPath = f.readline().strip()
      exportHost = f.readline().strip()
      pathsplit  = exportPath.split('/')

   f.close()
   conn.close()
   
def cleanupVirtualmachineFiles():
   #
   # IN vacd THIS FUNCTION CAN ONLY BE RUN INSIDE THE MAIN LOOP
   # ie the same level as where VacVM.createVM() is
   # called. Otherwise active directories may be deleted!
   # 

# should go through the directories present (VM and vmtype)
# rather than through the ones we know about. ones we don't
# know about should be deleted as they will be ones that are 
# no longer supported in this space (config file changed?)
# otherwise they may never be got rid of

   for vmname in virtualmachines:
   
     vm = VacVM(vmname)

     # we go through the vmtypes, looking for directory
     # hierarchies that aren't the current VM instance.
     # 'current' includes the last used hierarchy if the
     # VM state is shutdown
     for vmtypeName, vmtype in vmtypes.iteritems():
       try:
         dirslist = os.listdir('/var/lib/vac/machines/' + vmname + '/' + vmtypeName)
       except:
         continue

       for onedir in dirslist:
         if os.path.isdir('/var/lib/vac/machines/' + vmname + '/' + vmtypeName + '/' + onedir):
           
           # delete the current VM instance's big root.disk image file IF VM IS SHUTDOWN 
           if vm.uuidStr and vm.uuidStr == onedir and vm.state == VacState.shutdown:
             try:
               os.remove('/var/lib/vac/machines/' + vmname + '/' + vmtypeName + '/' + onedir + '/root.disk')
             except:
               pass

           # delete everything if not the current VM instance
           elif not vm.uuidStr or vm.uuidStr != onedir:
             shutil.rmtree('/var/lib/vac/machines/' + vmname + '/' + vmtypeName + '/' + onedir)
             logLine('Deleting /var/lib/vac/machines/' + vmname + '/' + vmtypeName + '/' + onedir)
   

                  