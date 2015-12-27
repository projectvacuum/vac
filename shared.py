#
#  shared.py - common functions, classes, and variables for Vac
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-5. All rights reserved.
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

import re
import os
import sys
import uuid
import time
import json
import glob
import errno
import base64
import shutil
import string
import hashlib
import StringIO
import urllib
import datetime
import tempfile
import socket
import stat

import pycurl
import libvirt
import ConfigParser

import vac

# All VacQuery requests and responses are in this file
# so we can define the VacQuery protocol version here
vacQueryVersion = '0.2'

natNetwork      = '169.254.0.0'
natNetmask      = '255.255.0.0'
natPrefix       = '169.254.169.'
factoryAddress  = '169.254.169.254'
mjfAddress      = '169.254.169.253'
udpBufferSize   = 16777216

overloadPerCpu = None
gocdbSitename = None

factories = None
hs06PerMachine = None
mbPerMachine = None
fixNetworking = None
forwardDev = None
shutdownTime = None

numVirtualmachines = None
numCpus = None
cpuCount = None
spaceName = None
udpTimeoutSeconds = None
vacVersion = None

cpuPerMachine = None
versionLogger = None
gbScratchesMeasured = None
machinetypes = None
vacmons = None

volumeGroup = None
gbScratch = None
machinefeaturesOptions = None

def readConf():
      global gocdbSitename, \
             factories, hs06PerMachine, mbPerMachine, fixNetworking, forwardDev, shutdownTime, \
             numVirtualmachines, numCpus, cpuCount, spaceName, udpTimeoutSeconds, vacVersion, \
             cpuPerMachine, versionLogger, gbScratchesMeasured, machinetypes, vacmons, \
             volumeGroup, gbScratch, overloadPerCpu, fixNetworking, machinefeaturesOptions

      # reset to defaults
      overloadPerCpu = 2.0
      gocdbSitename = None

      factories = []
      hs06PerMachine = None
      mbPerMachine = 2048
      fixNetworking = True
      forwardDev = None
      shutdownTime = None

      numVirtualmachines = None
      numCpus = None
      cpuCount = countProcProcessors()
      spaceName = None
      udpTimeoutSeconds = 5.0
      vacVersion = '0.0.0'

      cpuPerMachine = 1
      versionLogger = True
      gbScratchesMeasured = {}
      machinetypes = {}
      vacmons = []
      
      volumeGroup = 'vac_volume_group'
      gbScratch   = 40
      machinefeaturesOptions = {}

      try:
        f = open('/var/lib/vac/VERSION', 'r')
        vacVersion = f.readline().split('=',1)[1].strip()
        f.close()
      except:
        pass

      if not '.' in os.uname()[1]:
        return 'The hostname of the factory machine must be a fully qualified domain name!'
      
      parser = ConfigParser.RawConfigParser()

      # Look for configuration files in /etc/vac.d
      try:
        confFiles = os.listdir('/etc/vac.d')
      except:
        pass 
      else:
        for oneFile in sorted(confFiles):
          if oneFile[-5:] == '.conf':
            parser.read('/etc/vac.d/' + oneFile)

      # Standalone configuration file, read last in case of manual overrides
      parser.read('/etc/vac.conf')
      
      # general settings from [Settings] section

      if not parser.has_section('settings'):
        return 'Must have a settings section!'
      
      # Must have a space name
      if not parser.has_option('settings', 'vac_space'):
        return 'Must give vac_space in [settings]!'
        
      spaceName = parser.get('settings','vac_space').strip()

      if parser.has_option('settings', 'gocdb_sitename'):
        gocdbSitename = parser.get('settings','gocdb_sitename').strip()
        
      if parser.has_option('settings', 'domain_type'):
          print 'domain_type is deprecated - please remove from the Vac configuration!'          
          
      if parser.has_option('settings', 'cpu_total'):
          # Option limit on number of processors Vac can allocate.
          numCpus = int(parser.get('settings','cpu_total').strip())

          # Check setting against counted number
          if numCpus > cpuCount:
           return 'cpu_total cannot be greater than number of processors!'
      else:
          # Defaults to count from /proc/cpuinfo
          numCpus = cpuCount
                                                 
      if parser.has_option('settings', 'total_machines'):
          # Number of VMs for Vac to auto-define.
          numVirtualmachines = int(parser.get('settings','total_machines').strip())
      else:
          numVirtualmachines = cpuCount
                                                 
      if parser.has_option('settings', 'overload_per_cpu'):
          # Multiplier to calculate overload veto against creating more VMs
          overloadPerCpu = float(parser.get('settings','overload_per_cpu'))
             
      if parser.has_option('settings', 'volume_group'):
          # Volume group to search for logical volumes 
          volumeGroup = parser.get('settings','volume_group').strip()
             
      if parser.has_option('settings', 'scratch_gb'):
          # Size in GB (1000^3) of scratch volumes, 0 to disable, default is 40
          gbScratch = int(parser.get('settings','scratch_gb').strip())
             
      if parser.has_option('settings', 'udp_timeout_seconds'):
          # How long to wait before giving up on more UDP replies          
          udpTimeoutSeconds = float(parser.get('settings','udp_timeout_seconds').strip())

      if (parser.has_option('settings', 'fix_networking') and
          parser.get('settings','fix_networking').strip().lower() == 'false'):
           fixNetworking = False
      else:
           fixNetworking = True

      if parser.has_option('settings', 'forward_dev'):
           forwardDev = parser.get('settings','forward_dev').strip()

      if (parser.has_option('settings', 'version_logger') and
          parser.get('settings','version_logger').strip().lower() == 'false'):
           versionLogger = False
      else:
           versionLogger = True

      if parser.has_option('settings', 'vacmon_hostport'):
           try:
             vacmons = parser.get('settings','vacmon_hostport').lower().split()
           except:
             return 'Failed to parse vacmon_hostport'
             
           for v in vacmons:
             if re.search('^[a-z0-9.-]+:[0-9]+$', v) is None:
               return 'Failed to parse vacmon_hostport: must be host.domain:port'

      if parser.has_option('settings', 'delete_old_files'):
          print 'Old files are now always deleted: please remove delete_old_files from [settings]'
             
      if parser.has_option('settings', 'vcpu_per_machine'):
          # Warn that this deprecated
          cpuPerMachine = int(parser.get('settings','vcpu_per_machine'))
          print 'vcpu_per_machine is deprecated: please use cpu_per_machine in [settings]'
      elif parser.has_option('settings', 'cpu_per_machine'):
          # If this isn't set, then we allocate one cpu per VM
          cpuPerMachine = int(parser.get('settings','cpu_per_machine'))
             
      if parser.has_option('settings', 'mb_per_machine'):          
          mbPerMachine = int(parser.get('settings','mb_per_machine'))
# Start warning about this deprecation in the next release
#          print 'mb_per_machine is deprecated: please use mb_per cpu in [settings]'
      elif parser.has_option('settings', 'mb_per_cpu'):
          # If this isn't set, then we use default (2048 MiB)
          mbPerMachine = cpuPerMachine * int(parser.get('settings','mb_per_cpu'))

      if parser.has_option('settings', 'shutdown_time'):
        try:
          shutdownTime = int(parser.get('settings','shutdown_time'))
        except:
          return 'Failed to parse shutdown_time (must be a Unix time seconds date/time)'

      if parser.has_option('settings', 'hs06_per_machine'):
          hs06PerMachine = float(parser.get('settings','hs06_per_machine'))
          print 'hs06_per_machine is deprecated: please use hs06_per_cpu in [settings]'
      elif parser.has_option('settings', 'hs06_per_cpu'):
          hs06PerMachine = cpuPerMachine * float(parser.get('settings','hs06_per_cpu'))
      else:
          # If this isn't set, then we use the default 1.0 * cpuPerMachine
          hs06PerMachine = float(cpuPerMachine)

      try:
          # Get list of factory machines to query via UDP. Leave an empty list if none.
          factories = (parser.get('settings', 'factories')).lower().split()
      except:
          if parser.has_option('factories', 'names'):
            return 'Please use the factories option within [settings] rather than a separate [factories] section! See the Admin Guide for details.'

      # additional machinefeatures key/value pairs
      for (oneOption,oneValue) in parser.items('settings'):
         if oneOption[0:23] == 'machinefeatures_option_':
           if string.translate(oneOption, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
             return 'Name of machinefeatures_option_xxx (' + oneOption + ') must only contain a-z 0-9 and _'
           else:
             machinefeaturesOptions[oneOption[23:]] = parser.get('settings', oneOption)

      # all other sections are machinetypes (other types of section are ignored)
      for sectionName in parser.sections():

         sectionNameSplit = sectionName.lower().split(None,1)
         
         # For now, can still define these machinetype sections with [vmtype ...] too
         if sectionNameSplit[0] == 'machinetype' or sectionNameSplit[0] == 'vmtype':
         
             if string.translate(sectionNameSplit[1], None, '0123456789abcdefghijklmnopqrstuvwxyz-') != '':
                 return 'Name of machinetype section [machinetype ' + sectionNameSplit[1] + '] can only contain a-z 0-9 or -'
         
             machinetype = {}
             machinetype['root_image'] = parser.get(sectionName, 'root_image')

             if parser.has_option(sectionName, 'cernvm_signing_dn'):
                 machinetype['cernvm_signing_dn'] = parser.get(sectionName, 'cernvm_signing_dn').strip()

             if parser.has_option(sectionName, 'target_share'):
                 machinetype['share'] = float(parser.get(sectionName, 'target_share'))
             elif parser.has_option('targetshares', sectionNameSplit[1]):
                 return "Please use a target_shares option within [machinetype " + sectionNameSplit[1] + "] rather than a separate [targetshares] section. You can still group target shares together or put them in a separate file: see the Admin Guide for details."
             else:
                 machinetype['share'] = 0.0
                                            
             if parser.has_option(sectionName, 'machine_model'):
                 machinetype['machine_model'] = parser.get(sectionName, 'machine_model')
             elif parser.has_option(sectionName, 'vm_model'):
                 # Deprecation warnings next time!
                 machinetype['machine_model'] = parser.get(sectionName, 'vm_model')
             else:
                 machinetype['machine_model'] = 'cernvm3'
             
             if parser.has_option(sectionName, 'root_device'):
                 machinetype['root_device'] = parser.get(sectionName, 'root_device')
             else:
                 machinetype['root_device'] = 'vda'
             
             if parser.has_option(sectionName, 'scratch_device'):
                 machinetype['scratch_device'] = parser.get(sectionName, 'scratch_device')
             else:
                 machinetype['scratch_device'] = 'vdb'

             if parser.has_option(sectionName, 'rootpublickey'):
                 print 'The rootpublickey option is deprecated; please use root_public_key!'
                 machinetype['root_public_key'] = parser.get(sectionName, 'rootpublickey')
             elif parser.has_option(sectionName, 'root_public_key'):
                 machinetype['root_public_key'] = parser.get(sectionName, 'root_public_key')

             if parser.has_option(sectionName, 'user_data'):
                 machinetype['user_data'] = parser.get(sectionName, 'user_data')
             else:
                 return 'user_data is now required in each machinetype section!'

             if parser.has_option(sectionName, 'log_machineoutputs'):
                 print 'log_machineoutputs has been deprecated: please use machines_dir_days to control this'
             
             if parser.has_option(sectionName, 'machines_dir_days'):
                 machinetype['machines_dir_days'] = float(parser.get(sectionName, 'machines_dir_days'))
             elif parser.has_option(sectionName, 'machineoutputs_days'):
                 # Deprecated; warn in next release
                 machinetype['machines_dir_days'] = float(parser.get(sectionName, 'machineoutputs_days'))
             else:
                 machinetype['machines_dir_days'] = 3.0
             
             if parser.has_option(sectionName, 'max_wallclock_seconds'):
                 machinetype['max_wallclock_seconds'] = int(parser.get(sectionName, 'max_wallclock_seconds'))
             else:
                 machinetype['max_wallclock_seconds'] = 86400
             
             if parser.has_option(sectionName, 'min_wallclock_seconds'):
                 machinetype['min_wallclock_seconds'] = int(parser.get(sectionName, 'min_wallclock_seconds'))
             else:
                 machinetype['min_wallclock_seconds'] = machinetype['max_wallclock_seconds']

             if parser.has_option(sectionName, 'backoff_seconds'):
                 machinetype['backoff_seconds'] = int(parser.get(sectionName, 'backoff_seconds'))
             else:
                 machinetype['backoff_seconds'] = 10
             
             if parser.has_option(sectionName, 'fizzle_seconds'):
                 machinetype['fizzle_seconds'] = int(parser.get(sectionName, 'fizzle_seconds'))
             else:
                 machinetype['fizzle_seconds'] = 600
            
             if parser.has_option(sectionName, 'heartbeat_file'):
                 machinetype['heartbeat_file'] = parser.get(sectionName, 'heartbeat_file')

             if parser.has_option(sectionName, 'heartbeat_seconds'):
                 machinetype['heartbeat_seconds'] = int(parser.get(sectionName, 'heartbeat_seconds'))
             else:
                 machinetype['heartbeat_seconds'] = 0
             
             if parser.has_option(sectionName, 'accounting_fqan'):
                 machinetype['accounting_fqan'] = parser.get(sectionName, 'accounting_fqan')
             
             for (oneOption,oneValue) in parser.items(sectionName):

                 if (oneOption[0:17] == 'user_data_option_') or (oneOption[0:15] == 'user_data_file_'):

                   if string.translate(oneOption, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
                     return 'Name of user_data_option_xxx (' + oneOption + ') must only contain a-z 0-9 and _'
                   else:              
                     machinetype[oneOption] = parser.get(sectionName, oneOption)                
             
             if parser.has_option(sectionName, 'user_data_proxy_cert') and \
                not parser.has_option(sectionName, 'user_data_proxy_key') :
                 return 'user_data_proxy_cert given but user_data_proxy_key missing (they can point to the same file if necessary)'
             elif not parser.has_option(sectionName, 'user_data_proxy_cert') and \
                  parser.has_option(sectionName, 'user_data_proxy_key') :
                 return 'user_data_proxy_key given but user_data_proxy_cert missing (they can point to the same file if necessary)'
             elif parser.has_option(sectionName, 'user_data_proxy_cert') and \
                  parser.has_option(sectionName, 'user_data_proxy_key') :
                 machinetype['user_data_proxy_cert'] = parser.get(sectionName, 'user_data_proxy_cert')
                 machinetype['user_data_proxy_key']  = parser.get(sectionName, 'user_data_proxy_key')
             
             if parser.has_option(sectionName, 'legacy_proxy') and \
                parser.get(sectionName,'legacy_proxy').strip().lower() == 'true':
                 machinetype['legacy_proxy'] = True
             else:
                 machinetype['legacy_proxy'] = False
             
             machinetypes[sectionNameSplit[1]] = machinetype
                          
      # Finished successfully, with no error to return
      return None

def nameFromOrdinal(ordinal):
      nameParts = os.uname()[1].split('.',1)
      return nameParts[0] + '-%02d' % ordinal + '.' + nameParts[1]

def ipFromOrdinal(ordinal):
      return natPrefix + str(ordinal)

def loadAvg(which = 0):
      # By default, use [0], the one minute load average
      avg = 0.0
      
      try:
        f = open('/proc/loadavg')
      except:
        print 'Failed to open /proc/loadavg'
        return avg
        
      avg = float(f.readline().split()[which])
      
      f.close()
      return avg

def memInfo():
   # Get some interesting quantities out of /proc/meminfo
   result = {}
   
   try:
     f = open('/proc/meminfo', 'r')
   except:
     print 'Failed to open /proc/meminfo'
     return None

   while True:
     fields = f.readline().split()
     
     if len(fields) == 0:
       break
     
     if fields[0] == 'SwapTotal:':
       result['SwapTotal'] = int(fields[1])
     elif fields[0] == 'SwapFree:':
       result['SwapFree'] = int(fields[1])
     elif fields[0] == 'MemTotal:':
       result['MemTotal'] = int(fields[1])
     elif fields[0] == 'MemFree:':
       result['MemFree'] = int(fields[1])

   f.close()

   if 'SwapTotal' in result and \
      'SwapFree'  in result and \
      'MemTotal'  in result and \
      'MemFree'   in result:
     return result
   else:
     return None

def countProcProcessors():
      numProcessors = 0

      try:
        f = open('/proc/cpuinfo','r')
      except:
        print 'Failed to open /proc/cpuinfo'
        return numProcessors

      oneLine = f.readline()      
      while oneLine:

         if oneLine.startswith('processor\t:'):
           numProcessors += 1

         oneLine = f.readline()

      f.close()      
      return numProcessors

def setSockBufferSize(sock):

   try:
     if int(open('/proc/sys/net/core/rmem_max', 'r').readline().strip()) < udpBufferSize:
       open('/proc/sys/net/core/rmem_max', 'w').write(str(udpBufferSize) + '\n')
   except:
     pass

   try:
     sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, udpBufferSize)
   except:
     vac.vacutils.logLine('Failed setting RCVBUF to %d' % udpBufferSize)
   
def canonicalFQDN(hostName):
   if '.' in hostName:
     # Assume ok if already contains '.'
     return hostName
     
   try:
     # Try to get DNS domain from current host's FQDN
     return hostName + '.' + os.uname()[1].split('.',1)[1]
   except:
     # If failed, then just return what we were given
     return hostName
              
class VacState:
   unknown, shutdown, starting, running, paused, zombie = ('Unknown', 'Shut down', 'Starting', 'Running', 'Paused', 'Zombie')

class VacVM:
   def __init__(self, ordinal):
      self.ordinal          = ordinal
      self.name             = nameFromOrdinal(ordinal)
      self.state            = VacState.unknown
      self.started          = None
      self.finishedFile     = None
      self.cpuSeconds       = 0
      self.cpus             = cpuPerMachine
      self.mbPerMachine     = None
      self.hs06PerMachine   = None

      try:
        createdStr, self.machinetypeName, self.uuidStr = open('/var/lib/vac/slots/' + self.name,'r').read().split()
        self.created = int(createdStr)
      except:
        self.uuidStr         = None
        self.machinetypeName = None
        self.created         = None

      conn = libvirt.open(None)
      if conn == None:
        vac.vacutils.logLine('Failed to open connection to the hypervisor')
        raise

      try:
        dom = conn.lookupByName(self.name)
        domState = dom.info()[0]
      except:
        dom = None
        domState = None

      conn.close()
      
      if dom and self.uuidStr != dom.UUIDString():
        # if VM exists but doesn't match slot's UUID, then a zombie, to be killed
        self.state = VacState.zombie
        return

      if not self.created or not self.uuidStr or not self.machinetypeName:
        # if created, UUID, and machinetype not (never?) properly set then stop now
        self.state = VacState.shutdown
        return
        
      try:
        self.started = int(os.stat(self.machinesDir() + '/started').st_ctime)
      except:
        self.started = None
        self.state = VacState.starting
                          
      if dom:
      
        if domState == libvirt.VIR_DOMAIN_RUNNING or domState == libvirt.VIR_DOMAIN_BLOCKED:
          self.state = VacState.running

        else:
          self.state = VacState.paused
          vac.vacutils.logLine('!!! libvirt state is ' + str(domState) + ', setting VacState.paused !!!')

        try:
          self.cpuSeconds = int(dom.info()[4] / 1000000000.0)
        except:
          pass
            
      else:
        self.state = VacState.shutdown
         
        try:
          f = open(self.machinesDir() + '/heartbeat', 'r')
          self.cpuSeconds = int(f.readline().split(' ')[0])
          f.close()
        except:
          pass

      try:
        self.heartbeat = int(os.stat(self.machinesDir() + '/heartbeat').st_ctime)
      except:
        self.heartbeat = None

      try: 
        self.shutdownMessage = open(self.machinesDir() + '/joboutputs/shutdown_message', 'r').read().strip()
        self.timeShutdownMessage = int(os.stat(self.machinesDir() + '/joboutputs/shutdown_message').st_ctime)
      except:
        self.shutdownMessage = None
        self.timeShutdownMessage = None
                        
      try: 
        self.shutdownTime = int(open(self.machinesDir() + '/machinefeatures/shutdowntime', 'r').read().strip())
      except:
        self.shutdownTime = None
      else:
        if shutdownTime and (shutdownTime < self.shutdownTime):
          # need to reduce shutdowntime in the VM
          try:
            vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/shutdowntime',
                                    str(shutdownTime), 
                                    stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

            vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/shutdowntime_job',
                                    str(shutdownTime), 
                                    stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
          except:
               pass

      try:
        self.joboutputsHeartbeat = int(os.stat(self.machinesDir() + '/joboutputs/' + 
                                               machinetypes[self.machinetypeName]['heartbeat_file']).st_mtime)
      except:
        self.joboutputsHeartbeat = None

      try: 
        self.cpus = int(open(self.machinesDir() + '/jobfeatures/allocated_CPU', 'r').read().strip())
      except:
        pass
      
      try: 
        self.hs06 = float(open(self.machinesDir() + '/machinefeatures/hs06', 'r').read().strip())
      except:
        self.hs06 = hs06PerMachine
      
      try: 
        # we use our mem_limit_bytes to avoid usage of 1000^2 for memory in MJF mem_limit_MB spec
        self.mb = (int(open(self.machinesDir() + '/jobfeatures/mem_limit_bytes', 'r').read().strip()) / 1048576)
      except:
        self.mb = mbPerMachine
      
      if self.uuidStr and os.path.exists(self.machinesDir() 
                                         + '/finished') :
        self.finishedFile = True
      else:
        self.finishedFile = False

   def machinesDir(self):
      return '/var/lib/vac/machines/' + str(self.created) + ':' + self.machinetypeName + ':' + self.uuidStr

   def createHeartbeatFile(self):
      self.heartbeat = int(time.time())
      
      try:
        f = open('/var/lib/vac/machines/' + str(self.created) + ':' + self.machinetypeName + ':' + self.uuidStr + '/heartbeat', 'r')
        lastCpuSeconds = int(f.readline().split(' ')[0])
        f.close()
        
        lastHeartbeat = int(os.stat('/var/lib/vac/machines/' + str(self.created) + ':' +
                                    self.machinetypeName + ':' + self.uuidStr + '/heartbeat').st_ctime)
                                    
        cpuPercentage = 100.0 * float(self.cpuSeconds - lastCpuSeconds) / (self.heartbeat - lastHeartbeat)
        heartbeatLine = str(self.cpuSeconds) + (" %.1f" % cpuPercentage)
      except:
        heartbeatLine = str(self.cpuSeconds)

      try:
        vac.vacutils.createFile('/var/lib/vac/machines/' + str(self.created) + ':' + self.machinetypeName + ':' +
                                self.uuidStr + '/heartbeat', heartbeatLine + '\n', stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
      except:
        pass
                                  
   def createFinishedFile(self):
      try:
        f = open('/var/lib/vac/machines/' + str(self.created) + ':' + self.machinetypeName + ':' + self.uuidStr + '/finished', 'w')
        f.close()
      except:
        vac.vacutils.logLine('Failed creating /var/lib/vac/machines/' + str(self.created) + ':' + self.machinetypeName + ':' + self.uuidStr + '/finished')

   def writeApel(self):
      # Write accounting information about a VM that has finished
      if self.state != VacState.shutdown or not self.started or not self.heartbeat:
        return
                
      # Ignore machinetypes we don't know about
      if self.machinetypeName not in machinetypes:
        return

      # If the VM just ran for fizzle_seconds, then we don't log it
      if (self.heartbeat - self.started) < machinetypes[self.machinetypeName]['fizzle_seconds']:
        return
        
      nowTime = time.gmtime()

      try:
        os.makedirs(time.strftime('/var/lib/vac/apel-outgoing/%Y%m%d', nowTime), stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      except:
        pass

      try:
        os.makedirs(time.strftime('/var/lib/vac/apel-archive/%Y%m%d', nowTime), stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      except:
        pass
      
      userDN = ''
      for component in spaceName.split('.'):
        userDN = '/DC=' + component + userDN
        
      if 'accounting_fqan' in machinetypes[self.machinetypeName]:
        userFQANField = 'FQAN: ' + machinetypes[self.machinetypeName]['accounting_fqan'] + '\n'
      else:
        userFQANField = ''

      if gocdbSitename:
        tmpGocdbSitename = gocdbSitename
      else:
        tmpGocdbSitename = spaceName

      mesg = ('APEL-individual-job-message: v0.3\n' + 
              'Site: ' + tmpGocdbSitename + '\n' +
              'SubmitHost: ' + spaceName + '/vac-' + os.uname()[1] + '\n' +
              'LocalJobId: ' + self.uuidStr + '\n' +
              'LocalUserId: ' + os.uname()[1] + '\n' +
              'Queue: ' + self.machinetypeName + '\n' +
              'GlobalUserName: ' + userDN + '\n' +
              userFQANField +
              'WallDuration: ' + str(self.heartbeat - self.started) + '\n' +
              'CpuDuration: ' + str(self.cpuSeconds) + '\n' +
              'Processors: ' + str(self.cpus) + '\n' +
              'NodeCount: 1\n' +
              'InfrastructureDescription: APEL-VAC\n' +
              'InfrastructureType: grid\n' +
              'StartTime: ' + str(self.started) + '\n' +
              'EndTime: ' + str(self.heartbeat) + '\n' +
              'MemoryReal: ' + str(self.mb * 1024) + '\n' +
              'MemoryVirtual: ' + str(self.mb * 1024) + '\n' +
              'ServiceLevelType: HEPSPEC\n' +
              'ServiceLevel: ' + str(self.hs06) + '\n' +
              '%%\n')
                          
      fileName = time.strftime('%H%M%S', nowTime) + str(time.time() % 1)[2:][:8]
                          
      try:
        vac.vacutils.createFile(time.strftime('/var/lib/vac/apel-archive/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
      except:
        vac.vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vac/apel-archive/%Y%m%d/', nowTime) + fileName)
        return

      if gocdbSitename:
        # We only write the outgoing copy if gocdb_sitename is explicitly given
        try:
          vac.vacutils.createFile(time.strftime('/var/lib/vac/apel-outgoing/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
        except:
          vac.vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vac/apel-outgoing/%Y%m%d/', nowTime) + fileName)
          return

   def destroy(self):
       conn = libvirt.open(None)
       if conn == None:
         print 'Failed to open connection to the hypervisor'
         return
                                  
       try:
         dom = conn.lookupByUUIDString(self.uuidStr)
         dom.destroy()
       except:
         pass
   
   def makeOpenStackData(self):
   
      if 'user_data' in machinetypes[self.machinetypeName]:
        try:
          self.setupUserDataContents()        
        except Exception as e:
          raise NameError('Failed to create user_data (' + str(e) + ')')

      metaData = {}

      if 'root_public_key' in machinetypes[self.machinetypeName]:

        if machinetypes[self.machinetypeName]['root_public_key'][0] == '/':
          root_public_key_file = machinetypes[self.machinetypeName]['root_public_key']
        else:
          root_public_key_file = '/var/lib/vac/machinetypes/' + self.machinetypeName + '/' + machinetypes[self.machinetypeName]['root_public_key']
          
        try:
          publicKey = open(root_public_key_file, 'r').read()
        except:
          raise NameError('Failed to read ' + root_public_key_file)
        else:
          metaData['public_keys'] = { "0" : publicKey }

          try:
            open(self.machinesDir() + '/root_public_key','w').write(publicKey)
          except:
            raise NameError('Failed to create root_public_key')
                      
      metaData['uuid']              = self.uuidStr
      metaData['availability_zone'] = spaceName
      metaData['hostname']          = self.name
      metaData['name']              = self.name
      
      metaData['meta'] = {
                           'machinefeatures' : 'http://' + mjfAddress + '/machinefeatures',
                           'jobfeatures'     : 'http://' + mjfAddress + '/jobfeatures',
                           'joboutputs'      : 'http://' + mjfAddress + '/joboutputs',
                           'machinetype'     : self.machinetypeName
                         }
      
      try:
        vac.vacutils.createFile('/var/lib/vac/machines/' + str(self.created) + ':' + self.machinetypeName + ':' + self.uuidStr + '/meta_data.json',
                                json.dumps(metaData),
                                stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
      except:
        raise NameError('Failed to create meta_data.json')

   def makeMJF(self):
      os.makedirs(self.machinesDir() + '/machinefeatures', stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      os.makedirs(self.machinesDir() + '/jobfeatures',     stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      os.makedirs(self.machinesDir() + '/joboutputs',      stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

      # Standard machinefeatures

      # HEPSPEC06 per virtual machine
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/hs06',
                 str(hs06PerMachine), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # we don't know the physical vs logical cores distinction here so we just use cpu
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/phys_cores',
                 str(cpuPerMachine), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # again just use cpu
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/log_cores',
                 str(cpuPerMachine), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # tell them they have the whole VM to themselves; they are in the only jobslot here
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/jobslots',
                '1', stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Use earlier shutdown time if given in config
      now = int(time.time())
      tmpShutdownTime = int(now + machinetypes[self.machinetypeName]['max_wallclock_seconds'])
      
      if shutdownTime and (shutdownTime < tmpShutdownTime):
        tmpShutdownTime = shutdownTime
      
      cpuLimitSecs = tmpShutdownTime - now
      if (cpuLimitSecs < 0):
        cpuLimitSecs = 0

      # calculate the absolute shutdown time for the VM, as a machine
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/shutdowntime',
                 str(tmpShutdownTime),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # additional machinefeatures options defined in configuration
      if machinefeaturesOptions:
        for oneOption,oneValue in machinefeaturesOptions.iteritems():
          vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/' + oneOption,
                                  oneValue,
                                  stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Standard  jobfeatures
      
      # calculate the absolute shutdown time for the VM, as a job
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/shutdowntime_job',
                 str(tmpShutdownTime),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # we don't do this, so just say 1.0 for cpu factor
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/cpufactor_lrms',
                 '1.0', stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # for the scaled and unscaled cpu limit, we use the wallclock seconds multiple by the cpu
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/cpu_limit_secs_lrms',
                 str(cpuLimitSecs * cpuPerMachine),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/cpu_limit_secs',
                 str(cpuLimitSecs * cpuPerMachine),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # for the scaled and unscaled wallclock limit, we use the wallclock seconds without factoring in cpu
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/wall_limit_secs_lrms',
                 str(cpuLimitSecs),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/wall_limit_secs',
                 str(cpuLimitSecs),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # if we know the size of the scratch partition, we use it as the disk_limit_GB (1000^3 not 1024^3 bytes)
      if self.name in gbScratchesMeasured:
         vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/disk_limit_GB',
                 str(gbScratchesMeasured[self.name]), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # we are about to start the VM now
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/jobstart_secs',
                 str(now), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # mbPerMachine is in units of 1024^2 bytes, whereas jobfeatures wants 1000^2!!!
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/mem_limit_MB',
                 str((mbPerMachine * 1048576) / 1000000), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/mem_limit_bytes',
                 str(mbPerMachine * 1048576), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
                        
      # cpuPerMachine again
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/allocated_CPU',
                 str(cpuPerMachine) + '\n', stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')


   def setupUserDataContents(self):
 
      tmpShutdownTime = int(time.time() + machinetypes[self.machinetypeName]['max_wallclock_seconds'])
      
      if shutdownTime and (shutdownTime < tmpShutdownTime):
        tmpShutdownTime = shutdownTime
        
      try:
        userDataContents = vac.vacutils.createUserData(
                                               shutdownTime       = tmpShutdownTime,
                                               machinetypePath	  = '/var/lib/vac/machinetypes/' + self.machinetypeName,
                                               options		  = machinetypes[self.machinetypeName],
                                               versionString	  = 'Vac ' + vacVersion,
                                               spaceName	  = spaceName, 
                                               machinetypeName	  = self.machinetypeName, 
                                               userDataPath	  = machinetypes[self.machinetypeName]['user_data'], 
                                               hostName		  = self.name, 
                                               uuidStr		  = self.uuidStr,
                                               machinefeaturesURL = 'http://' + mjfAddress + '/machinefeatures',
                                               jobfeaturesURL     = 'http://' + mjfAddress + '/jobfeatures',
                                               joboutputsURL      = 'http://' + mjfAddress + '/joboutputs')
      except Exception as e:
        raise NameError('Failed to read ' + machinetypes[self.machinetypeName]['user_data'] + ' (' + str(e) + ')')

      try:
        o = open(self.machinesDir() + '/user_data', 'w')
        o.write(userDataContents)
        o.close()
      except:
        raise NameError('Failed writing to ' + self.machinesDir() + '/user_data')
      
   def makeRootDisk(self):

      # Exceptions have to be caught by function calling makeRootDisk()
      fTmp, fileName = tempfile.mkstemp(prefix = 'root.disk.', dir = '/var/lib/vac/tmp')

      # CernVM 3 just needs a big empty file
      if self.model == 'cernvm3':
         vac.vacutils.logLine('Make 70 GB sparse file ' + fileName)
         try:
          f = open(fileName, 'ab')
          f.truncate(70 * 1014 * 1024 * 1024)
          f.close()
          return fileName
         except:
          raise NameError('creation of sparse disk image fails!')

      elif self.model == 'vm-raw':

         if machinetypes[self.machinetypeName]['root_image'][0:7] == 'http://' or machinetypes[self.machinetypeName]['root_image'][0:8] == 'https://':
           try:
              rawFileName = vac.vacutils.getRemoteRootImage(machinetypes[self.machinetypeName]['root_image'], '/var/lib/vac/imagecache', '/var/lib/vac/tmp')
           except Exception as e:
              return str(e)
         elif machinetypes[self.machinetypeName]['root_image'][0] == '/':
           rawFileName = machinetypes[self.machinetypeName]['root_image']
         else:
           rawFileName = '/var/lib/vac/machinetypes/' + self.machinetypeName + '/' + machinetypes[self.machinetypeName]['root_image']

         if 'cernvm_signing_dn' in machinetypes[self.machinetypeName]:
           cernvmDict = vac.vacutils.getCernvmImageData(rawFileName)
           if cernvmDict['verified'] == False:
             return 'Failed to verify signature/cert for ' + rawFileName
           elif re.search(machinetypes[self.machinetypeName]['cernvm_signing_dn'],  cernvmDict['dn']) is None:
             return 'Signing DN ' + cernvmDict['dn'] + ' does not match cernvm_signing_dn = ' + machinetypes[self.machinetypeName]['cernvm_signing_dn']
           else:
             vac.vacutils.logLine('Verified image signed by ' + cernvmDict['dn'])

         # Make a small QEMU qcow2 disk for this instance,
         # backed by the full image given in conf
         if os.system('qemu-img create -b ' + rawFileName + ' -f qcow2 ' + fileName + ' >/dev/null') != 0:
          raise NameError('creation of copy-on-write disk image fails!')
         else:
          return fileName

#      elif self.model == 'monolithic':
#         # Just copy the root.disk.
#         # To avoid long startups, the source should ideally be a sparse file too.
#         vac.vacutils.logLine('copy from ' + machinetypes[self.machinetypeName]['root_image'] + ' to ' + fileName)
#         if os.system('/bin/cp --force --sparse=always ' + machinetypes[self.machinetypeName]['root_image'] + ' ' + fileName) != 0:
#          raise NameError('copy of disk image fails!')
#         else:
#          return fileName

      return None

   def measureScratchDisk(self):

      try:
       # get logical volume size in GB (1000^3 not 1024^3)
       f = os.popen('/sbin/lvs --nosuffix --units G --noheadings -o lv_size /dev/' + volumeGroup + '/' + self.name + ' 2>/dev/null', 'r')
       sizeGB = float(f.readline())
       f.close()
       gbScratchesMeasured[self.name] = sizeGB
      except:
       vac.vacutils.logLine('failed to read size of /dev/' + volumeGroup + '/' + self.name + ' using lvs command')
       pass      

   def destroyVM(self):
      conn = libvirt.open(None)
      if conn == None:
          vac.vacutils.logLine('Failed to open connection to the hypervisor')
          raise NameError('failed to open connection to the hypervisor')

      try:
        dom = conn.lookupByName(self.name)
        dom.shutdown()
        time.sleep(30.0)
        dom.destroy()
      except:
        pass

      self.state = VacState.shutdown

      conn.close()

   def createVM(self, machinetypeName):
      self.model           = machinetypes[machinetypeName]['machine_model']
      self.created         = int(time.time())
      self.machinetypeName = machinetypeName
      self.uuidStr         = str(uuid.uuid4())

      os.makedirs(self.machinesDir(), stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

# Should do this somewhere else...
      try:
        os.makedirs('/var/lib/vac/slots', 
                  stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      except:
        pass

      vac.vacutils.createFile('/var/lib/vac/slots/' + self.name,
                              str(self.created) + ' ' + self.machinetypeName + ' ' + self.uuidStr,
                              stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')

      vac.vacutils.createFile(self.machinesDir() + '/name', self.name,
                              stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')

      if self.name in gbScratchesMeasured:

        if not os.path.exists('/dev/' + volumeGroup + '/' + self.name):
            vac.vacutils.logLine('Trying to create scratch logical volume for ' + self.name + ' in ' + volumeGroup)
            os.system('LVM_SUPPRESS_FD_WARNINGS=1 /sbin/lvcreate --name ' + self.name + ' -L ' + str(gbScratch) + 'G ' + volumeGroup + ' 2>&1')

        try:
            if not stat.S_ISBLK(os.stat('/dev/' + volumeGroup + '/' + self.name).st_mode):
              return 'failing due to /dev/' + volumeGroup + '/' + self.name + ' not a block device'
        except:
            return 'failing due to /dev/' + volumeGroup + '/' + self.name + ' not existing'

        self.measureScratchDisk()
        scratch_volume_xml = ("<disk type='block' device='disk'>\n" +
                              " <driver name='qemu' type='raw' error_policy='report' cache='unsafe'/>\n" +
                              " <source dev='/dev/" + volumeGroup + "/" + self.name  + "'/>\n" +
                              " <target dev='" + machinetypes[self.machinetypeName]['scratch_device'] + 
                              "' bus='" + ("virtio" if "vd" in machinetypes[self.machinetypeName]['scratch_device'] else "ide") + "'/>\n</disk>")
      else:
        scratch_volume_xml = ""

      if self.model != 'cernvm3':
          cernvm_cdrom_xml    = ""
      else:
          # For cernvm3 need to set up the ISO boot image
          if machinetypes[self.machinetypeName]['root_image'][0:7] == 'http://' or machinetypes[self.machinetypeName]['root_image'][0:8] == 'https://':          
            try:
              cernvmCdrom = vac.vacutils.getRemoteRootImage(machinetypes[self.machinetypeName]['root_image'], '/var/lib/vac/imagecache', '/var/lib/vac/tmp')
            except Exception as e:
              return str(e)
          elif machinetypes[self.machinetypeName]['root_image'][0] == '/':
            cernvmCdrom = machinetypes[self.machinetypeName]['root_image']
          else:
            cernvmCdrom = '/var/lib/vac/machinetypes/' + self.machinetypeName + '/' + machinetypes[self.machinetypeName]['root_image']

          if 'cernvm_signing_dn' in machinetypes[self.machinetypeName]:
            cernvmDict = vac.vacutils.getCernvmImageData(cernvmCdrom)
            if cernvmDict['verified'] == False:
              return 'Failed to verify signature/cert for ' + cernvmCdrom            
            elif re.search(machinetypes[self.machinetypeName]['cernvm_signing_dn'],  cernvmDict['dn']) is None:
              return 'Signing DN ' + cernvmDict['dn'] + ' does not match cernvm_signing_dn = ' + machinetypes[self.machinetypeName]['cernvm_signing_dn']
            else:
              vac.vacutils.logLine('Verified image signed by ' + cernvmDict['dn'])
      
          cernvm_cdrom_xml = ("<disk type='file' device='cdrom'>\n" +
                              " <driver name='qemu' type='raw' error_policy='report' cache='unsafe'/>\n" +
                              " <source file='" + cernvmCdrom  + "'/>\n" +
                              " <target dev='hdc' />\n<readonly />\n</disk>")

      ip      = natPrefix + str(self.ordinal)
      ipBytes = ip.split('.')
      mac     = '56:4D:%02X:%02X:%02X:%02X' % (int(ipBytes[0]), int(ipBytes[1]), int(ipBytes[2]), int(ipBytes[3]))

      vac.vacutils.logLine('Using MAC ' + mac + ' when creating ' + self.name)

      try:
        self.makeMJF()
      except Exception as e:
        return 'Failed making MJF files (' + str(e) + ')'

      try:
        self.makeOpenStackData()
      except Exception as e:
        return 'Failed making OpenStack meta_data (' + str(e) + ')'
        
      try:
          conn = libvirt.open(None)
      except:
          return 'exception when opening connection to the hypervisor'

      if conn == None:
          return 'failed to open connection to the hypervisor'
                
      try:
        rootDiskFileName = self.makeRootDisk()
      except Exception as e:
        return 'Failed to make root disk image (' + str(e) + ')'
        
      if os.path.isfile("/usr/libexec/qemu-kvm"):
            qemuKvmFile = "/usr/libexec/qemu-kvm"
      elif os.path.isfile("/usr/bin/qemu-kvm"):
            qemuKvmFile = "/usr/bin/qemu-kvm"
      else:
            return "qemu-kvm not in /usr/libexec or /usr/bin!"
      
      xmldesc=( """<domain type='kvm'>
  <name>""" + self.name + """</name>
  <uuid>""" + self.uuidStr + """</uuid>
  <memory unit='MiB'>""" + str(mbPerMachine) + """</memory>
  <currentMemory unit='MiB'>"""  + str(mbPerMachine) + """</currentMemory>
  <vcpu>""" + str(cpuPerMachine) + """</vcpu>
  <os>
    <type arch='x86_64' machine='pc'>hvm</type>
    <boot dev='cdrom'/>
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
    <emulator>""" + qemuKvmFile + """</emulator>
    <disk type='file' device='disk'>""" + 
    ("<driver name='qemu' type='qcow2' cache='unsafe' error_policy='report' />" if (self.model=='vm-raw') else "<driver name='qemu' cache='unsafe' type='raw' error_policy='report' />") + 
    """<source file='""" + rootDiskFileName + """' /> 
     <target dev='""" + machinetypes[self.machinetypeName]['root_device'] + """' bus='""" + 
     ("virtio" if "vd" in machinetypes[self.machinetypeName]['root_device'] else "ide") + """'/>
    </disk>""" + scratch_volume_xml + cernvm_cdrom_xml + """
    <controller type='usb' index='0'>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x01' function='0x2'/>
    </controller>
    <interface type='network'>
      <mac address='""" + mac + """'/>
      <source network='vac_""" + natNetwork + """'/>
      <model type='virtio'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x03' function='0x0'/>
      <filterref filter='clean-traffic'/>
    </interface>
    <serial type="file">
      <source path="/var/lib/vac/machines/"""  + str(self.created) + ':' + self.machinetypeName + ':' + self.uuidStr + """/console.log"/>
      <target port="1"/>
    </serial>                    
    <graphics type='vnc' port='"""  + str(5900 + self.ordinal) + """' keymap='en-gb'><listen type='address' address='127.0.0.1'/></graphics>
    <video>
      <model type='vga' vram='9216' heads='1'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x02' function='0x0'/>
    </video>
    <memballoon model='virtio'>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x05' function='0x0'/>
    </memballoon>
  </devices>
</domain>
""" )

      vac.vacutils.createFile('/var/lib/vac/machines/' + str(self.created) + ':' + self.machinetypeName + ':' + self.uuidStr + '/started',
                  str(int(time.time())) + '\n', stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
      
      vac.vacutils.createFile('/var/lib/vac/machines/' + str(self.created) + ':' + self.machinetypeName + ':' + self.uuidStr + '/heartbeat',
                 '0.0 0.0\n', stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
      
      try:
           dom = conn.createXML(xmldesc, 0)
      except Exception as e:
           vac.vacutils.logLine('Exception ("' + str(e) + '") when trying to create VM domain for ' + self.name)
           conn.close()
           return 'exception when trying to create VM domain'
      finally:
           # We unlink the big, sparse root disk image once libvirt has it open too, 
           # so it disappears when libvirt is finished with it
           os.remove(rootDiskFileName)
           
      if not dom:
           vac.vacutils.logLine('Failed when trying to create VM domain for ' + self.name)
           conn.close()
           return 'failed when trying to create VM domain'
           
      conn.close()
       
      self.state = VacState.running

      # Everything ok so return no error message
      return None

def checkNetwork():
      # Check and if necessary create network and set its attributes

      conn = libvirt.open(None)
      
      try:
           # Find the network is already defined
           vacNetwork = conn.networkLookupByName('vac_' + natNetwork)
      except:
           # Doesn't exist so we define and start it
           vac.vacutils.logLine('No libvirt network vac_' + natNetwork + ' defined for NAT') 
           
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

           netXML = "<network>\n <name>vac_" + natNetwork + "</name>\n <forward mode='nat'"
           
           if forwardDev:
             netXML += " dev='" + forwardDev + "'"
           
           netXML += "/>\n <ip address='" + factoryAddress + "' netmask='" + natNetmask + "'>\n"
           netXML += "  <dhcp>\n" + dhcpXML + "</dhcp>\n </ip>\n</network>\n"
      
           try:
             vacNetwork = conn.networkDefineXML(netXML)
           except Exception as e:  
             vac.vacutils.logLine('Failed to define network vac_' + natNetwork + ' due to "' + str(e) + '"')
             return False
           else:
             vac.vacutils.logLine('Defined network vac_' + natNetwork)

      # Check the network is actually running, not just defined    
      if not vacNetwork.isActive():    
           try:  
             # Try starting it if not running
             vacNetwork.create()
           except Exception as e:  
             vac.vacutils.logLine('Starting defined network vac_' + natNetwork + ' fails with "' + str(e) + '"')
             vac.vacutils.logLine('Do you need to install dnsmasq RPM >= 2.48-13? Old "dnsmasq --listen-address ' + factoryAddress + '" process still running? Did you disable Zeroconf? Does virbr1 already exist?)')

             if fixNetworking:
               fixNetworkingCommands()

             return False
           else:  
             vac.vacutils.logLine('Started previously defined network vac_' + natNetwork)

      # Check the network is set to auto-start
      if not vacNetwork.autostart():
           try:
             # Try setting autostart
             vacNetwork.setAutostart(True)
           except Exception as e:
             vac.vacutils.logLine('Failed to set autostart for network vac_' + natNetwork + ' due to "' + str(e) + '"')
             return False
           else:
             vac.vacutils.logLine('Set auto-start for network vac_' + natNetwork)

      # Make sure that the dummy0 interface exists
      # Should still return 0 even if dummy0 already exists, with any IP
      if os.system('/sbin/ifconfig dummy0 ' + mjfAddress) != 0:
        vac.vacutils.logLine('(Re)run of ifconfig dummy0 ' + mjfAddress + ' fails!')
        return False
        
      return True
     
def checkIpTables(bridgeName):
      # Do a quick check of the output of iptables-save, looking for
      # signs that the NAT rules we need are there and haven't been
      # removed by something like Puppet, and log the results.
      #
      # bridgeName should normally be virbr1 (libvirt makes virbr0)
      #
      
      anyMissing = False

      try:
        f = os.popen('/sbin/iptables-save', 'r')
        iptablesSave = f.read()
        f.close()
      except:
        vac.vacutils.logLine('Failed to run /sbin/iptables-save')
        return
      
      iptablesPatterns = [ 
                           '%s.*tcp.*MASQUERADE'           % natNetwork,
                           '%s.*udp.*MASQUERADE'           % natNetwork,
                           '%s.*udp.*ACCEPT'               % bridgeName,
                           '%s.*tcp.*ACCEPT'               % bridgeName,
                           '%s.*%s.*ACCEPT|%s.*%s.*ACCEPT' % (natNetwork, bridgeName, bridgeName, natNetwork),
                           '%s.*%s.*ACCEPT'                % (bridgeName, bridgeName),
                           '%s.*CHECKSUM'		   % bridgeName
                         ]
      
      for pattern in iptablesPatterns:
        if re.search(pattern, iptablesSave) is None:
          anyMissing = True
          vac.vacutils.logLine('Failed to match "%s" in output of iptables-save. Have the NAT rules been removed?' % pattern)

      if anyMissing:
        vac.vacutils.logLine('iptables NAT check failed for ' + bridgeName)
      else:
        vac.vacutils.logLine('iptables NAT check passed for ' + bridgeName)

def fixNetworkingCommands():
      # Called if network doesn't exist and creation fails. Almost always this is
      # due to a restart/upgrade of libvirt and the same things need doing.
      #
      # This feature can be disabled with fix_networking = false in vac.conf

      # We assume the libvirt defaults so the desired bridge is virbr1
      vac.vacutils.logLine('Trying to fix networking so can create virbr1 bridge in next cycle')
      
      try:
        cmd = '/sbin/ifconfig virbr1 down'
        vac.vacutils.logLine('Trying  ' + cmd)
        os.system(cmd)
      except:
        pass

      try:
        cmd = '/usr/sbin/brctl delbr virbr1'
        vac.vacutils.logLine('Trying  ' + cmd)
        os.system(cmd)
      except:
        pass
       
      try:
        cmd = '/bin/kill -9 `/bin/ps -C dnsmasq -o pid,args | /bin/grep -- "--listen-address ' + factoryAddress + '" | /bin/cut -f1 -d" "`'
        vac.vacutils.logLine('Trying  ' + cmd)
        os.system(cmd)
      except:
        pass

def cleanupOldMachines():

   machinesList = os.listdir('/var/lib/vac/machines')

   for machineDir in machinesList:
   
      try:
        createdStr, machinetypeName, uuidStr = machineDir.split('/')[-1]
      except:
        continue

      if machinetypeName not in machinetypes:
        # use 3 days for machinetypes that have been removed
        machines_dir_days = 3.0

      elif machinetypes[machinetypeName]['machines_dir_days'] == 0.0:
        # if zero then we do not expire these directories at all
        continue

      else:
        # use the per-machinetype value
        machines_dir_days = machinetypes[machinetypeName]['machines_dir_days']

        if (os.stat(machineDir + '/heartbeat').st_mtime < int(time.time() - machines_dir_days * 86400)):
          vac.vacutils.logLine('Deleting expired ' + machineDir)
          shutil.rmtree(machineDir)

def makeMjfBody(created, machinetypeName, uuidStr, path):

   if '/../' in path:
     # Obviously not ok
     return None

   # Fold // to /
   requestURI = path.replace('//','/')

   splitRequestURI = requestURI.split('/')

   if len(splitRequestURI) > 3:
     # Subdirectories are now allowed
     return None

   machinesDir = '/var/lib/vac/machines/' + str(created) + ':' + machinetypeName + ':' + uuidStr

   if requestURI == '/machinefeatures/' or \
      requestURI == '/machinefeatures' or \
      requestURI == '/jobfeatures/' or \
      requestURI == '/jobfeatures':
     # Make HTML directory listing
     try:
       body = '<html><body><ul>'

       for fileName in os.listdir(machinesDir + '/' + splitRequestURI[1]):
         body += '<li><a href="' + splitRequestURI[2] + '">' + splitRequestURI[2] + '</a></li>'
         
       body += '</ul></body></html>'
       return body
     except Exception as e:
       vac.vacutils.logLine('Failed to make directory listing for ' + machinesDir + '/' + splitRequestURI[1] + ' (' + str(e) + ')')
       return None

   elif (splitRequestURI[1] == 'machinefeatures' or splitRequestURI[1] == 'jobfeatures') and splitRequestURI[2]:
     # Return an individual MJF value
     try:
       return open(machinesDir + '/' + splitRequestURI[1] + '/' + splitRequestURI[2], 'r').read()
     except Exception as e:
       vac.vacutils.logLine('Failed to get MJF value from ' + machinesDir + '/' + splitRequestURI[1] + '/' + splitRequestURI[2] + ' (' + str(e) + ')')
       return None

   return None

def makeMetadataBody(created, machinetypeName, uuidStr, path):

   machinesDir = '/var/lib/vac/machines/' + str(created) + ':' + machinetypeName + ':' + uuidStr

   # Fold // to /, and /latest/ to something that will match a dated version
   requestURI = path.replace('//','/').replace('/latest/','/0000-00-00/')

   # EC2 or OpenStack user data
   if re.search('^/[0-9]{4}-[0-9]{2}-[0-9]{2}/user-data$|^/openstack/[0-9]{4}-[0-9]{2}-[0-9]{2}/user-data$', requestURI):
     try:
       return open(machinesDir + '/user_data', 'r').read()
     except Exception as e:
       return None

   # meta-data directory listing
   if re.search('^/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data/$|^/openstack/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data/$', requestURI):
     body = ''

     for fileName in ['public-keys/0/openssh-key', 'ami-id', 'instance-id']:
       body += fileName + '\n'
         
     return body

   # EC2 SSH key
   if re.search('^/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data/public-keys/0/openssh-key$', requestURI):
     try:
       return open(machinesDir + '/root_public_key', 'r').read()
     except:
       return None
   
   # Return UUID for EC2 instance-id, and for ami-id (at least it's something unique)
   if re.search('^/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data/ami-id$|^/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data/instance-id$', requestURI):
     try:
       return uuidStr
     except:
       return None

   # No body (and therefore 404) if we don't recognise the request
   return None

def writePutBody(created, machinetypeName, uuidStr, path, body):

   # Fold // to /
   requestURI = path.replace('//','/')
   
   splitRequestURI = requestURI.split('/')
   
   if len(splitRequestURI) != 3 or splitRequestURI[1] != 'joboutputs' or not splitRequestURI[2]:
     return False

   machinesDir = '/var/lib/vac/machines/' + str(created) + ':' + machinetypeName + ':' + uuidStr
   
   try:
     vac.vacutils.createFile(machinesDir + '/joboutputs/' + splitRequestURI[2],
                             body,
                             stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
   except Exception as e:
     vac.vacutils.logLine('Failed to write ' + machinesDir + '/joboutputs/' + splitRequestURI[2] + ' (' + str(e) + ')')
     return False

   vac.vacutils.logLine('Wrote ' + machinesDir + '/joboutputs/' + splitRequestURI[2])
   return True

def sendMachinetypesRequests(factoryList = None):

   salt = base64.b64encode(os.urandom(32))
   sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   sock.settimeout(1.0)
   setSockBufferSize(sock)

   # Initialise dictionary of per-factory, per-machinetype responses
   responses = {}

   if factoryList is None:
     factoryList = factories

   for rawFactoryName in factoryList:
     responses[canonicalFQDN(rawFactoryName)] = { 'machinetypes' : {} }

   timeCount = 0
   
   # We just use integer second counting for now, despite the config file
   while timeCount <= int(udpTimeoutSeconds):
     timeCount += 1

     requestsRequired = 0
     for rawFactoryName in factoryList:
     
       factoryName = canonicalFQDN(rawFactoryName)

       try:
         numMachinetypes = responses[factoryName]['num_machinetypes']
       except:
         # We initially expect every factory to tell us about at least 1 machinetype
         numMachinetypes = 1
     
       # Send out requests to all factories with insufficient replies so far
       if len(responses[factoryName]['machinetypes']) < numMachinetypes:

         requestsRequired += 1
         try:          
           sock.sendto(json.dumps({'vac_version'      : 'Vac ' + vacVersion,
                                   'vacquery_version' : 'VacQuery ' + vac.shared.vacQueryVersion,
                                   'space'            : spaceName,
                                   'cookie'           : hashlib.sha256(salt + factoryName).hexdigest(),
                                   'method'           : 'machinetypes'}),
                       (factoryName,995))

         except socket.error:
           pass

     if requestsRequired == 0:
       # We can stop early since we have received all the expected responses already
       break

     # Gather responses from all factories until none for 1.0 second
#NEED A LIMIT ON HOW LONG IN TOTAL TOO!?
#IF WE KEEP GETTING REPLIES THEN WE GO ROUND FOREVER
     while True:
   
         try:
           data, addr = sock.recvfrom(10240)
                      
           try:
             response = json.loads(data)
           except:
             vac.vacutils.logLine('json.loads failed for ' + data)
             continue

# should check types as well as presence!
           if 'method'			in response and \
              response['method'] == 'machinetype' and \
              'cookie' 			in response and \
              'space' 			in response and \
              response['space']  == spaceName and \
              'factory' 		in response and \
              response['cookie'] == hashlib.sha256(salt + response['factory']).hexdigest() and \
              'num_machinetypes'	in response and \
              'machinetype'		in response and \
              'total_hs06'		in response and \
              'num_before_fizzle'	in response and \
              'shutdown_message'	in response and \
              'shutdown_time'		in response and \
              'shutdown_machine'	in response:
              
             responses[response['factory']]['num_machinetypes'] = response['num_machinetypes']

             responses[response['factory']]['machinetypes'][response['machinetype']] = response

         except socket.error:
           # timed-out so stop gathering responses for now
           break

   return responses

def sendMachinesRequests(factoryList = None):

   salt = base64.b64encode(os.urandom(32))
   sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   sock.settimeout(1.0)
   setSockBufferSize(sock)

   # Initialise dictionary of per-factory, per-machine responses
   responses = {}

   if factoryList is None:
     factoryList = factories

   for rawFactoryName in factoryList:   
     responses[vac.shared.canonicalFQDN(rawFactoryName)] = { 'machines' : {} }

   timeCount = 0
   
   # We just use integer second counting for now, despite the config file
   while timeCount <= int(udpTimeoutSeconds):
     timeCount += 1

     requestsRequired = 0
     for rawFactoryName in factoryList:

       factoryName = canonicalFQDN(rawFactoryName)

       try:
         numMachines = responses[factoryName]['num_machines']
       except:
         # We initially expect every factory to tell us about at least 1 machine
         numMachines = 1
     
       # Send out requests to all factories with insufficient replies so far
       if len(responses[factoryName]['machines']) < numMachines:

         requestsRequired += 1
         try:          
           sock.sendto(json.dumps({'vac_version'      : 'Vac ' + vacVersion,
                                   'vacquery_version' : 'VacQuery ' + vac.shared.vacQueryVersion,
                                   'space'            : spaceName,
                                   'cookie'           : hashlib.sha256(salt + factoryName).hexdigest(),
                                   'method'           : 'machines'}),
                       (factoryName,995))

         except socket.error:
           pass

     if requestsRequired == 0:
       # We can stop early since we have received all the expected responses already
       break

     # Gather responses from all factories until none for 1.0 second
#NEED A LIMIT ON HOW LONG IN TOTAL TOO!?
     while True:
   
         try:
           data, addr = sock.recvfrom(10240)
                      
           try:
             response = json.loads(data)
           except:
             vac.vacutils.logLine('json.loads failed for ' + data)
             continue

# should check types as well as presence!
           if 'method'			in response and \
              response['method'] == 'machine' and \
              'cookie' 			in response and \
              'space' 			in response and \
              response['space']  == spaceName and \
              'factory' 		in response and \
              response['cookie'] == hashlib.sha256(salt + response['factory']).hexdigest() and \
              'num_machines'		in response and \
              'machine'			in response and \
              'state'			in response and \
              'uuid'			in response and \
              'created_time'		in response and \
              'started_time'		in response and \
              'heartbeat_time'		in response and \
              'cpu_seconds'		in response and \
              'cpu_percentage'		in response and \
              'hs06'			in response and \
              'machinetype'		in response and \
              'shutdown_message'	in response and \
              'shutdown_time'		in response:
              
             responses[response['factory']]['num_machines'] = response['num_machines']
             
             responses[response['factory']]['machines'][response['machine']] = response

         except socket.error:
           # timed-out so stop gathering responses for now
           break

   return responses

def sendFactoriesRequests(factoryList = None):

   salt = base64.b64encode(os.urandom(32))
   sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   sock.settimeout(1.0)
   setSockBufferSize(sock)

   # Initialise dictionary of per-factory responses
   responses = {}

   if factoryList is None:
     factoryList = factories

   timeCount = 0
   
   # We just use integer second counting for now, despite the config file
   while timeCount <= int(udpTimeoutSeconds):
     timeCount += 1

     requestsRequired = 0
     for rawFactoryName in factoryList:

       factoryName = canonicalFQDN(rawFactoryName)
     
       # Send out requests to all factories with insufficient replies so far
       if factoryName not in responses:

         requestsRequired += 1
         try:          
           sock.sendto(json.dumps({'vac_version'      : 'Vac ' + vacVersion,
                                   'vacquery_version' : 'VacQuery ' + vac.shared.vacQueryVersion,
                                   'space'            : spaceName,
                                   'cookie'           : hashlib.sha256(salt + factoryName).hexdigest(),
                                   'method'           : 'factories'}),
                       (factoryName,995))

         except socket.error:
           pass

     if requestsRequired == 0:
       # We can stop early since we have received all the expected responses already
       break

     # Gather responses from all factories until none for 1.0 second
#NEED A LIMIT ON HOW LONG IN TOTAL TOO!?
     while True:
   
         try:
           data, addr = sock.recvfrom(10240)
                      
           try:
             response = json.loads(data)
           except:
             vac.vacutils.logLine('json.loads failed for ' + data)
             continue

           if 'method'			in response and \
              response['method'] == 'factory' and \
              'cookie' 			in response and \
              'space' 			in response and \
              response['space']  == spaceName and \
              'factory' 		in response and \
              response['cookie'] == hashlib.sha256(salt + response['factory']).hexdigest() :
              
             responses[response['factory']] = response

         except socket.error:
           # timed-out so stop gathering responses for now
           break

   return responses

def makeMachineResponses(cookie):
   responses = []

   # Go through the machine slots, sending one message for each
   for ordinal in range(numVirtualmachines):

     name = nameFromOrdinal(ordinal)

     try:
       (createdStr, machinetypeName, uuidStr) = open('/var/lib/vac/slots/' + name,'r').read().split()
       created = int(createdStr)

     except:
       uuidStr         = None
       machinetypeName = None
       created         = None

     else:
       machinesDir = '/var/lib/vac/machines/' + str(created) + ':' + machinetypeName + ':' + uuidStr
# what if machinesDir doesn't exist? (eg has been cleaned up??)
                
       try:
         startedStat = os.stat(machinesDir + '/started')
         timeStarted = int(startedStat.st_ctime)
       except:
         timeStarted = None
                             
       try:
         heartbeatStat = os.stat(machinesDir + '/heartbeat')
         timeHeartbeat = int(heartbeatStat.st_ctime)
       except:
         timeHeartbeat = None

       try:                  
         f = open(machinesDir + '/machinefeatures/hs06', 'r')
         hs06 = float(f.readline().strip())
         f.close()
       except:
         hs06 = hs06PerMachine

       try:
         # this is written by Vac as it monitors the machine through libvirt
         oneLine = open(machinesDir + '/heartbeat', 'r').readline()
                                    
         cpuSeconds = int(oneLine.split(' ')[0])
         try:
           cpuPercentage = float(oneLine.split(' ')[1])
         except:
           cpuPercentage = None
                    
       except:
         cpuSeconds    = None
         cpuPercentage = None

       hasFinished = os.path.exists(machinesDir + '/finished')

       shutdownMessage     = None
       shutdownMessageTime = None

       # some hardcoded timeouts in case old files are left lying around 
       # this means that old files are ignored when working out the state
       if (timeStarted and 
           timeHeartbeat and 
           (timeHeartbeat > int(time.time() - 3600)) and
           not hasFinished):
         vmState = VacState.running
       elif not timeStarted and (created > int(time.time() - 3600)):
         vmState = VacState.starting
       else:
         vmState = VacState.shutdown

         try:
           shutdownMessage = open(machinesDir + '/joboutputs/shutdown_message').readline().strip()
           shutdownMessageTime = int(os.stat(machinesDir + '/joboutputs/shutdown_message').st_ctime)
         except:
           pass
                                                                                                                         
       vac.vacutils.logLine(name + ' is ' + str(vmState) + ' (' + str(machinetypeName) + ', started ' + str(created) + ')')

       responseDict = {
                'method'		: 'machine',
                'vac_version'		: 'Vac ' + vacVersion,
                'vacquery_version'	: 'VacQuery ' + vacQueryVersion,
                'cookie'	  	: cookie,
                'space'		    	: spaceName,
                'factory'       	: os.uname()[1],
                'num_machines'       	: numVirtualmachines,

                'machine' 		: name,
                'state'			: vmState,
                'uuid'			: uuidStr,
                'created_time'		: created,
                'started_time'		: timeStarted,
                'heartbeat_time'	: timeHeartbeat,
                'cpu_seconds'		: cpuSeconds,
                'cpu_percentage'	: cpuPercentage,
                'hs06' 		       	: hs06,
                'machinetype'		: machinetypeName,
                'shutdown_message'  	: shutdownMessage,
                'shutdown_time'     	: shutdownMessageTime
                      }

       if gocdbSitename:
         responseDict['gocdb_site'] = gocdbSitename

       responses.append(json.dumps(responseDict))                              

   return responses
   
def makeMachinetypeResponses(cookie):
   # Send back machinetype messages to the querying factory or client
   responses = []

   # Go through the machinetypes
   for machinetypeName in machinetypes:

     totalHS06               = 0.0
     numBeforeFizzle         = 0
     totalMachines           = 0

     # Go through the VM slots, looking for starting/running instances of this machinetype
     for ordinal in range(numVirtualmachines):

       name = nameFromOrdinal(ordinal)

       try:
         (createdStr, machinetypeNameTmp, uuidStr) = open('/var/lib/vac/slots/' + name,'r').read().split()
         created = int(createdStr)

       except:
         continue

       if machinetypeNameTmp != machinetypeName:
         continue

       machinesDir = '/var/lib/vac/machines/' + str(created) + ':' + machinetypeName + ':' + uuidStr
# what if machinesDir doesn't exist? (eg has been cleaned up??)

       try:
         timeStarted = int(os.stat(machinesDir + '/started').st_ctime)
       except:
         timeStarted = None

       try:
         timeHeartbeat = int(os.stat(machinesDir + '/heartbeat').st_ctime)
       except:
         timeHeartbeat = None

       try:                  
         hs06 = float(open(machinesDir + '/machinefeatures/hs06', 'r').readline())
       except:
         hs06 = hs06PerMachine

       hasFinished = os.path.exists(machinesDir + '/finished')

       # some hardcoded timeouts here in case old files are left lying around 
       # this means that old files are ignored when working out the state
       if (timeStarted and 
           timeHeartbeat and 
           (timeHeartbeat > int(time.time() - 3600)) and
           not hasFinished):
         # Running
         totalHS06     += hs06
         totalMachines += 1

         if int(time.time()) < timeStarted + machinetypes[machinetypeName]['fizzle_seconds']:
           numBeforeFizzle += 1

       elif not timeStarted and (created > int(time.time() - 3600)):
         # Starting
         totalHS06       += hs06
         totalMachines   += 1
         numBeforeFizzle += 1

     # Now go through older instances in /var/lib/vac/machines, looking for the most recently
     # created instance of this machinetype that has already finished
     
     finishedFilesList = glob.glob('/var/lib/vac/machines/*:' + machinetypeName + ':*/finished')
     finishedFilesList.sort()
     
     shutdownMessage     = None
     shutdownMessageTime = None
     shutdownMachineName = None

     if finishedFilesList:
       dir = '/'.join(finishedFilesList[-1].split('/')[:-1])

       try:
         shutdownMessage = open(dir + '/joboutputs/shutdown_message').readline().strip()
         messageCode = int(shutdownMessage[0:3])
         shutdownMessageTime = int(os.stat(dir + '/joboutputs/shutdown_message').st_ctime)
         shutdownMachineName = open(dir + '/name').readline().strip()
       except:
         # No explicit shutdown message with a message code, so we make one up if necessary
         
         try:
           timeStarted   = int(os.stat(dir + '/started').st_ctime)
           timeHeartbeat = int(os.stat(dir + '/heartbeat').st_ctime)
         except:
           pass
         else:
           if (timeHeartbeat - timeStarted) < machinetypes[machinetypeName]['fizzle_seconds']:
             try:
               shutdownMachineName = open(dir + '/name').readline().strip()
             except:
               pass
             else:
               shutdownMessageTime = timeHeartbeat
               shutdownMessage = '300 Vac detects fizzle after ' + str(timeHeartbeat - timeStarted) + ' seconds'

     responseDict = {
                'method'		: 'machinetype',
                'vac_version'		: 'Vac ' + vacVersion,
                'vacquery_version'	: 'VacQuery ' + vacQueryVersion,
                'cookie'	  	: cookie,
                'space'		    	: spaceName,
                'factory'       	: os.uname()[1],
                'num_machinetypes'      : len(machinetypes),

                'machinetype'		: machinetypeName,
                'total_hs06'        	: totalHS06,
                'total_machines'        : totalMachines,
                'num_before_fizzle' 	: numBeforeFizzle,
                'shutdown_message'  	: shutdownMessage,
                'shutdown_time'     	: shutdownMessageTime,
                'shutdown_machine'  	: shutdownMachineName
                     }

     if gocdbSitename:
       responseDict['gocdb_site'] = gocdbSitename
       
     try:
       responseDict['fqan'] = machinetypes[machinetypeName]['accounting_fqan']
     except:
       pass

     responses.append(json.dumps(responseDict))

   return responses
   
def makeFactoryResponse(cookie):
   # Send back factory status message to the querying client

   vacDiskStatFS  = os.statvfs('/var/lib/vac')
   rootDiskStatFS = os.statvfs('/tmp')
   
   memory = memInfo()

   try:
     counts = open('/var/lib/vac/counts','r').readline().split()
     runningMachines = int(counts[0])
     runningCpus     = int(counts[2])
   except:
     runningCpus     = 0
     runningMachines = 0

   responseDict = {
                'method'		   : 'factory',
                'vac_version'		   : 'Vac ' + vacVersion,
                'vacquery_version'	   : 'VacQuery ' + vacQueryVersion,
                'cookie'	  	   : cookie,
                'space'		    	   : spaceName,
                'factory'       	   : os.uname()[1],
                'time_sent'		   : int(time.time()),
                'total_cpus'		   : numCpus,
                'running_cpus'             : runningCpus,
                'total_machines'           : numVirtualmachines,
                'running_machines'         : runningMachines,
                'total_hs06'		   : numCpus * hs06PerMachine / cpuPerMachine,
                'vac_disk_avail_kb'        : ( vacDiskStatFS.f_bavail *  vacDiskStatFS.f_frsize) / 1024,
                'root_disk_avail_kb'       : (rootDiskStatFS.f_bavail * rootDiskStatFS.f_frsize) / 1024,
                'vac_disk_avail_inodes'    :  vacDiskStatFS.f_favail,
                'root_disk_avail_inodes'   : rootDiskStatFS.f_favail,
                'load_average'		   : loadAvg(2),
                'kernel_version'	   : os.uname()[2],
                'factory_heartbeat_time'   : int(os.stat('/var/lib/vac/factory-heartbeat').st_ctime),
                'responder_heartbeat_time' : int(os.stat('/var/lib/vac/responder-heartbeat').st_ctime),
                'swap_used_kb'		   : memory['SwapTotal'] - memory['SwapFree'],
                'swap_free_kb'		   : memory['SwapFree'],
                'mem_used_kb'		   : memory['MemTotal'] - memory['MemFree'],
                'mem_total_kb'		   : memory['MemTotal']
                  }

   if gocdbSitename:
     responseDict['gocdb_site'] = gocdbSitename

   return json.dumps(responseDict)

def publishGlueStatus():
# Gather data about this space and its machinetypes
#  responses = vac.shared.sendMachinesRequests(factoryList)
#  responses = vac.shared.sendMachinetypesRequests(factoryList)
#  responses = vac.shared.sendFactoriesRequests(factoryList)
# Count up the required totals
# Create and publish 2.0 JSON
# Create and publish 2.1 JSON
     
    responses = vac.shared.sendMachinesRequests(factoryList)
    responses = vac.shared.sendMachinetypesRequests(factoryList)
    responses = vac.shared.sendFactoriesRequests(factoryList)

def createGlueStatus(glueVersion = '2.0', runningMachines = 0, totalMachines = 0):
    """Return a GLUE 2.0/2.1 JSON string describing this space's status"""

    if glueVersion == '2.0':
      attributePrefix          = ''
      jobsOrVM                 = 'Jobs'
    elif glueVersion == '2.1':
      attributePrefix          = 'Cloud'
      jobsOrVM                 = 'VM'
    else:
      return

    glue2 = { attributePrefix + 'ComputingService' : [ {
                     'CreationTime'       : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                     'ID'                 : 'urn:glue2:' + attributePrefix + 'ComputingService:' + spaceName,
                     'Name'               : 'Vac',
                     'Type'               : 'uk.ac.gridpp.vac',
                     'QualityLevel'       : 'production',
                     'Running' + jobsOrVM : self.runningMachines,
                     'Total' + jobsOrVM   : self.totalMachines
                                                       } ]
            }
                 
    computingShares           = []
    mappingPolicies           = []
    executionEnvironments     = []
    cloudComputeInstanceTypes = []

    for machinetypeName in self.machinetypes:

      # (Cloud)ComputingShare
      
      tmpDict = {
                    'Associations'   : { 'ServiceID' : 'urn:glue2:' + attributePrefix + 'ComputingService:' + self.spaceName },
                    'CreationTime'   : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'ID'             : 'urn:glue2:' + attributePrefix + 'ComputingShare:' + self.spaceName + ':' + machinetypeName,
                    'Name'           : machinetypeName,
                    'Running' + jobsOrVM : self.machinetypes[machinetypeName].runningMachines,
                    'ServingState'   : 'production'
                }

      if glueVersion == '2.0':
        tmpDict['MaxWallTime']    = self.machinetypes[machinetypeName].max_wallclock_seconds
        tmpDict['MaxCPUTime']     = self.machinetypes[machinetypeName].max_wallclock_seconds
        tmpDict['MaxTotalJobs']   = self.machinetypes[machinetypeName].max_machines
        tmpDict['MaxRunningJobs'] = self.machinetypes[machinetypeName].max_machines
        tmpDict['TotalJobs']      = self.machinetypes[machinetypeName].totalMachines
      elif glueVersion == '2.1':
        tmpDict['MaxVM']          = self.machinetypes[machinetypeName].max_machines
        tmpDict['TotalVM']        = self.machinetypes[machinetypeName].totalMachines

      computingShares.append(tmpDict)

      # Mapping policy

      try:
        vo = self.machinetypes[machinetypeName].accounting_fqan.split('/')[1]
      except:
        pass
      else:                
        mappingPolicies.append({
                    'Associations'   : { 'ShareID' : 'urn:glue2:' + attributePrefix + 'ComputingShare:' + self.spaceName + ':' + machinetypeName },
                    'CreationTime'   : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'ID'             : 'urn:glue2:MappingPolicy:' + self.spaceName + ':' + machinetypeName,
                    'Name'           : machinetypeName,
                    'Rule'           : [ 'VO:' + vo, 'VOMS:' + self.machinetypes[machinetypeName].accounting_fqan ],
                    'Scheme'         : 'org.glite.standard'
                               })

      # ExecutionEnvironment or CloudComputeInstanceType

      if glueVersion == '2.0':
        executionEnvironments.append({
                    'Associations'   : { 'ShareID' : 'urn:glue2:ComputingShare:' + self.spaceName + ':' + machinetypeName },
                    'CreationTime'   : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'ID'             : 'urn:glue2:ExecutionEnvironment:' + self.spaceName + ':' + machinetypeName,
                    'Name'           : machinetypeName,
                    'Platform'       : 'x86_64',
                    'OSFamily'       : 'linux',
                    'OSName'         : 'CernVM 3'
                                     })
      elif glueVersion == '2.1':
        cloudComputeInstanceTypes.append({
                    'Associations'   : { 'ShareID' : 'urn:glue2:CloudComputingShare:' + self.spaceName + ':' + machinetypeName },
                    'CreationTime'   : time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'ID'             : 'urn:glue2:CloudComputeInstanceType:' + self.spaceName + ':' + machinetypeName,
                    'Name'           : machinetypeName,
                    'Platform'       : 'x86_64'
                                         })

    if len(computingShares) > 0:
      glue2[attributePrefix + 'ComputingShare'] = computingShares

      if len(mappingPolicies) > 0:
        glue2['MappingPolicy'] = mappingPolicies
        
      if len(executionEnvironments) > 0:
        glue2['ExecutionEnvironment'] = executionEnvironments

      if len(cloudComputeInstanceTypes) > 0:
        glue2['CloudComputeInstanceType'] = cloudComputeInstanceTypes

    try:
      vac.vacutils.createFile('/var/lib/vcycle/spaces/' + self.spaceName + '/glue-' + glueVersion + '.json', json.dumps(self.glue2), 
                                 0644, '/var/lib/vcycle/tmp')
    except:
      vacvacutils.logLine('Failed writing GLUE ' + glueVersion + ' JSON to /var/lib/vcycle/spaces/' + self.spaceName + '/glue-' + glueVersion + '.json')

          