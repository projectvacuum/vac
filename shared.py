#
#  shared.py - common functions, classes, and variables for Vac
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-8. All rights reserved.
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
import pwd
import uuid
import time
import glob
import errno
import ctypes
import base64
import shutil
import string
import signal
import hashlib
import subprocess
import StringIO
import urllib
import datetime
import tempfile
import socket
import stat

import pycurl
import libvirt
import ConfigParser

import json
json.encoder.FLOAT_REPR = lambda f: ("%.2f" % f)

import vac

class VacError(Exception):
  pass

# All VacQuery requests and responses are in this file
# so we can define the VacQuery protocol version here.
# 01.00 is the one described in HSF-TN-2016-04
# 01.01 has daemon_* and processor renames 
# 01.02 adds num_processors to machine_status
# 01.03 adds machine_model to machine_status 
vacQueryVersion = '01.03'

vmModels = [ 'cernvm3', 'cernvm4', 'vm-raw' ] # Virtual Machine models
dcModels = [ 'docker' ]                       # Docker Container models
scModels = [ 'singularity' ]                  # Singularity Container models
lmModels = vmModels + dcModels + scModels     # All Logical Machine models

dockerPath	    = '/usr/bin/docker'
singularityPath     = '/usr/bin/singularity'
natNetwork          = '169.254.0.0'
natNetmask          = '255.255.0.0'
natPrefix           = '169.254.169.'
metaAddress         = '169.254.169.254'
mjfAddress          = '169.254.169.253'
factoryAddress      = mjfAddress
dummyAddress        = metaAddress
udpBufferSize       = 16777216
gbDiskPerProcessorDefault = 40
singularityUser     = None
singularityUid      = None
singularityGid      = None
cpuCgroupFsRoot     = None
memoryCgroupFsRoot  = None

overloadPerProcessor = None
gocdbSitename = None
gocdbCertFile = None
gocdbKeyFile = None
gocdbUpdateSeconds = 86400
censusUpdateSeconds = 3600

factories = None
hs06PerProcessor = None
mbPerProcessor = None
fixNetworking = None
forwardDev = None
shutdownTime = None
draining = None

numMachineSlots = None
numProcessors = None
processorCount = None
spaceName = None
spaceDesc = None
udpTimeoutSeconds = None
vacqueryTries = 5
vacVersion = None

processorsPerSuperslot = None
versionLogger = None
machinetypes = None
vacmons = None
rootPublicKeyFile = None

volumeGroup = None
gbDiskPerProcessor = None
machinefeaturesOptions = None

def readConf(includePipes = False, updatePipes = False, checkVolumeGroup = False, printConf = False):
      global gocdbSitename, gocdbCertFile, gocdbKeyFile, \
             factories, hs06PerProcessor, mbPerProcessor, fixNetworking, forwardDev, shutdownTime, draining, \
             numMachineSlots, numProcessors, processorCount, spaceName, spaceDesc, udpTimeoutSeconds, vacVersion, \
             processorsPerSuperslot, versionLogger, machinetypes, vacmons, rootPublicKeyFile, \
             singularityUser, singularityUid, singularityGid, \
             volumeGroup, gbDiskPerProcessor, overloadPerProcessor, fixNetworking, machinefeaturesOptions

      # reset to defaults
      overloadPerProcessor = 1.25
      gocdbSitename = None
      gocdbCertFile = None
      gocdbKeyFile = None

      factories = []
      hs06PerProcessor = None
      mbPerProcessor = 2048
      fixNetworking = True
      forwardDev = None
      shutdownTime = None
      draining = False

      processorCount = countProcProcessors()
      numMachineSlots = processorCount
      numProcessors = None
      spaceName = None
      spaceDesc = None
      udpTimeoutSeconds = 10.0
      vacVersion = '0.0.0'

      processorsPerSuperslot = 1
      versionLogger = 1
      machinetypes = {}
      vacmons = []
      rootPublicKeyFile = '/root/.ssh/id_rsa.pub'
      singularityUser = None
        
      volumeGroup = None
      machinefeaturesOptions = {}
      
      # Temporary dictionary of common user_data_option_XXX 
      machinetypeCommon = {}

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

      # Standalone configuration file, read after vac.d in case of manual overrides
      parser.read('/etc/vac.conf')
      
      # Very last configuration file, which will be deleted at next boot
      parser.read('/var/run/vac.conf')
      
      # general settings from [Settings] section

      if not parser.has_section('settings'):
        return 'Must have a settings section!'
      
      # Must have a space name
      if not parser.has_option('settings', 'vac_space'):
        return 'Must give vac_space in [settings]!'
        
      spaceName = parser.get('settings','vac_space').strip()

      if parser.has_option('settings', 'description'):
        spaceDesc = parser.get('settings','description').strip()
        
      if parser.has_option('settings', 'gocdb_sitename'):
        gocdbSitename = parser.get('settings','gocdb_sitename').strip()
        
      if parser.has_option('settings', 'gocdb_cert_file'):
        gocdbCertFile = parser.get('settings','gocdb_cert_file').strip()
        
      if parser.has_option('settings', 'gocdb_key_file'):
        gocdbKeyFile = parser.get('settings','gocdb_key_file').strip()
        
      if parser.has_option('settings', 'domain_type'):
          print 'domain_type is deprecated - please remove from the Vac configuration!'          
          
      if parser.has_option('settings', 'cpu_total'):
          # Option limit on number of processors Vac can allocate.
          numProcessors = int(parser.get('settings','cpu_total').strip())
          print 'cpu_total is deprecated - please use total_processors instead!'

          # Check setting against counted number
          if numProcessors > processorCount:
           return 'cpu_total cannot be greater than number of processors!'
      elif parser.has_option('settings', 'total_processors'):
          # Option limit on number of processors Vac can allocate.
          numProcessors = int(parser.get('settings','total_processors').strip())

          # Check setting against counted number
          if numProcessors > processorCount:
           return 'total_processors cannot be greater than number of processors!'
      else:
          # Defaults to count from /proc/cpuinfo
          numProcessors = processorCount

      if parser.has_option('settings', 'total_machines'):
          print 'total_machines is deprecated. Please use total_processors in [settings] to control number of VMs'
                                                 
      if parser.has_option('settings', 'overload_per_cpu'):
          # Multiplier to calculate overload veto against creating more VMs
          print 'overload_per_cpu is deprecated - please use overload_per_processor!'
          overloadPerProcessor = float(parser.get('settings','overload_per_cpu'))
      elif parser.has_option('settings', 'overload_per_processor'):
          # Multiplier to calculate overload veto against creating more VMs
          overloadPerProcessor = float(parser.get('settings','overload_per_processor'))
             
      if parser.has_option('settings', 'singularity_user'):
          singularityUser = parser.get('settings','singularity_user').strip()
          try:
            pwdStruct = pwd.getpwnam(singularityUser)
            singularityUid = pwdStruct[2]
            singularityGid = pwdStruct[3]
          except:
            return 'Singularity user %s does not exist!' % singularityUser
            
          if singularityUid == 0:
            return 'You cannot use root as the Singularity user!'

      if parser.has_option('settings', 'volume_group'):
          # Volume group to search for logical volumes 
          volumeGroup = parser.get('settings','volume_group').strip()

      if checkVolumeGroup:
          if volumeGroup:
            if not measureVolumeGroup(volumeGroup):
              # If volume_group is given, then it must exist
              return 'Specified volume_group %s does not exist!' % volumeGroup
          elif measureVolumeGroup('vac_volume_group'):
              # If volume_group is not given, then it's ok if default does not exist
              # but we use it if it does exist
              volumeGroup = 'vac_volume_group'

      if parser.has_option('settings', 'scratch_gb'):
          # Deprecated
          gbDiskPerProcessor = int(parser.get('settings','scratch_gb').strip())
          print 'scratch_gb is deprecated. Please use disk_gb_per_processor in [settings] instead'
      elif parser.has_option('settings', 'disk_gb_per_cpu'):
          # Size in GB/cpu (1000^3) of disk assigned to machines, default is 40
          gbDiskPerProcessor = int(parser.get('settings','disk_gb_per_cpu').strip())
          print 'disk_gb_per_cpu is deprecated - please use disk_gb_per_processor!'
      elif parser.has_option('settings', 'disk_gb_per_processor'):
          # Size in GB/cpu (1000^3) of disk assigned to machines, default is 40
          gbDiskPerProcessor = int(parser.get('settings','disk_gb_per_processor').strip())

      if parser.has_option('settings', 'udp_timeout_seconds'):
          # How long to wait before giving up on more UDP replies          
          udpTimeoutSeconds = float(parser.get('settings','udp_timeout_seconds').strip())

      if (parser.has_option('settings', 'fix_networking') and
          parser.get('settings','fix_networking').strip().lower() == 'false'):
           fixNetworking = False
      else:
           fixNetworking = True

      if parser.has_option('settings', 'root_public_key'):
        rootPublicKeyFile = parser.get('settings', 'root_public_key').strip()

      if parser.has_option('settings', 'forward_dev'):
           forwardDev = parser.get('settings','forward_dev').strip()

      if parser.has_option('settings', 'version_logger'):
        # deprecated true/false then integer messages per day
        if parser.get('settings','version_logger').strip().lower() == 'true':
           print 'version_logger in [settings] now takes an integer rather true/false'
           versionLogger = 1
        elif parser.get('settings','version_logger').strip().lower() == 'false':
           print 'version_logger in [settings] now takes an integer rather true/false'
           versionLogger = 0
        else:
           try:
             versionLogger = int(parser.get('settings','version_logger').strip())
           except:
             versionLogger = 0

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
          processorsPerSuperslot = int(parser.get('settings','vcpu_per_machine'))
          print 'vcpu_per_machine is deprecated: please use processors_per_superslot in [settings]'
      elif parser.has_option('settings', 'cpu_per_machine'):
          processorsPerSuperslot = int(parser.get('settings','cpu_per_machine'))
          print 'cpu_per_machine is deprecated: please use processors_per_superslot in [settings]'

      if parser.has_option('settings', 'processors_per_superslot'):
          # If this isn't set, then we allocate one cpu per superslot
          processorsPerSuperslot = int(parser.get('settings','processors_per_superslot'))
                        
      if parser.has_option('settings', 'mb_per_cpu'):
          # If this isn't set, then we use default (2048 MiB)
          mbPerProcessor = int(parser.get('settings','mb_per_cpu'))
          print 'mb_per_cpu is deprecated - please use mb_per_processor!'
      elif parser.has_option('settings', 'mb_per_processor'):
          # If this isn't set, then we use default (2048 MiB)
          mbPerProcessor = int(parser.get('settings','mb_per_processor'))

      if parser.has_option('settings', 'shutdown_time'):
        try:
          shutdownTime = int(parser.get('settings','shutdown_time'))
        except:
          return 'Failed to parse shutdown_time (must be a Unix time seconds date/time)'

      if parser.has_option('settings', 'draining'):
        if parser.get('settings','draining').lower() == 'yes':
          draining = True

      if parser.has_option('settings', 'hs06_per_cpu'):
          hs06PerProcessor = float(parser.get('settings','hs06_per_cpu'))
          print 'hs06_per_cpu is deprecated - please use hs06_per_processor!'
      elif parser.has_option('settings', 'hs06_per_processor'):
          hs06PerProcessor = float(parser.get('settings','hs06_per_processor'))
      else:
          # If this isn't set, then we will use the default 1.0 per processor
          hs06PerProcessor = None

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

      # set up commmon user_data_option_XXX subsitutions for all machinetypes
      for (oneOption,oneValue) in parser.items('settings'):
        if (oneOption[0:17] == 'user_data_option_') or (oneOption[0:15] == 'user_data_file_'):
          if string.translate(oneOption, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
            return 'Name of user_data_option_xxx (' + oneOption + ') in [settings] must only contain a-z 0-9 and _'
          else:              
            machinetypeCommon[oneOption] = parser.get('settings', oneOption)

      # find and process vacuum_pipe sections
      if includePipes:
        for sectionName in parser.sections():
          sectionNameSplit = sectionName.lower().split(None,1)

          if sectionNameSplit[0] == 'vacuum_pipe':
          
            machinetypeNamePrefix = sectionNameSplit[1]

            if string.translate(machinetypeNamePrefix, None, '0123456789abcdefghijklmnopqrstuvwxyz-') != '':
              return 'Name of vacuum_pipe section [vacuum_pipe ' + machinetypeNamePrefix + '] can only contain a-z 0-9 or -'
              
            try:
              vacuumPipeURL = parser.get(sectionName, 'vacuum_pipe_url').strip()
            except:
              return 'Section [vacuum_pipe ' + machinetypeNamePrefix + '] must contain a vacuum_pipe_url option!'

            try:
              totalTargetShare = float(parser.get(sectionName, 'target_share').strip())
            except:
              totalTargetShare = 0.0

            try:
              vacuumPipe = vac.vacutils.readPipe('/var/lib/vac/pipescache/' + machinetypeNamePrefix + '.pipe',
                                                  vacuumPipeURL, 'Vac ' + vacVersion, updatePipes = updatePipes)
            except Exception as e:
              # If a vacuum pipe is given but cannot be read then skip
              print "Cannot read vacuum_pipe_url (" + vacuumPipeURL + ": " + str(e) + ") - no machinetypes created!"
              continue

            if not 'machinetypes' in vacuumPipe:
              continue
            
            # Process the contents of this pipe: "machinetypes" is a list of dictionaries, one per machinetype
            totalPipeTargetShare = 0.0
              
            # First pass to get total target shares
            for pipeMachinetype in vacuumPipe['machinetypes']:
              try:
                totalPipeTargetShare += float(pipeMachinetype['target_share'])
              except:
                pass
                  
            # Second pass to add options to the relevant machinetype sections
            for pipeMachinetype in vacuumPipe['machinetypes']:

              try:
                suffix = str(pipeMachinetype['suffix'])
              except:
                print "suffix is missing from one machinetype within " + vacuumPipeURL + " - skipping!"
                continue
                
              try:
                parser.add_section('machinetype ' + machinetypeNamePrefix + '-' + suffix)
              except:
                # Ok if it already exists
                pass

              # Copy almost all options from vacuum_pipe section to this new machinetype
              # unless they have already been given. Skip vacuum_pipe_url and target_share                  
              for n,v in parser.items(sectionName):
                if n != 'vacuum_pipe_url' and n != 'target_share' and \
                   not parser.has_option('machinetype ' + machinetypeNamePrefix + '-' + suffix, n):
                  parser.set('machinetype ' + machinetypeNamePrefix + '-' + suffix, n, v)
                                    
              # Record path to machinetype used to find the files on local disk
              parser.set('machinetype ' + machinetypeNamePrefix + '-' + suffix, 
                         'machinetype_path', '/var/lib/vac/machinetypes/' + machinetypeNamePrefix)
                
              acceptedOptions = [
                                    'accounting_fqan',
                                    'backoff_seconds',
                                    'container_command',
                                    'cvmfs_repositories',
                                    'fizzle_seconds',
                                    'disk_gb_per_processor',
                                    'heartbeat_file',
                                    'heartbeat_seconds',
                                    'image_signing_dn',
                                    'legacy_proxy',
                                    'machine_model',
                                    'max_processors',
                                    'max_wallclock_seconds',
                                    'min_processors',
                                    'min_wallclock_seconds',
                                    'root_device',
                                    'root_image',
                                    'scratch_device',
                                    'suffix',
                                    'target_share',                                    
                                    'tmp_binds',
                                    'user_data',
                                    'user_data_proxy'
                                ]

              # Go through vacuumPipe adding options if not already present from configuration files
              for optionRaw in pipeMachinetype:
                option = str(optionRaw)
                value  = str(pipeMachinetype[optionRaw])
                
                # Skip if option already exists - configuration files take precedence
                if parser.has_option('machinetype ' + machinetypeNamePrefix + '-' + suffix, option):
                  continue
                    
                # Already dealt with
                if option == 'suffix':
                  continue
                    
                # Deal with subdividing the total target share for this vacuum pipe here
                # Each machinetype gets a share based on its target_share within the pipe
                # We do the normalisation of the pipe target_shares here
                if option == 'target_share':
                  try:
                    targetShare = totalTargetShare * (float(value) / totalPipeTargetShare)
                  except:
                    targetShare = 0.0

                  parser.set('machinetype ' + machinetypeNamePrefix + '-' + suffix, 'target_share', str(targetShare))
                  continue
                    
                # Check option is one we accept
                if not option.startswith('user_data_file_' ) and \
                   not option.startswith('user_data_option_' ) and \
                   not option in acceptedOptions:
                  print 'Option %s is not accepted from vacuum pipe - ignoring!' % option
                  continue
                      
                # Any options which specify filenames on the hypervisor must be checked here  
                if (option.startswith('user_data_file_' )  or
                    option ==         'cvmfs_repositories' or
                    option ==         'heartbeat_file'   ) and '/' in value:
                  print 'Option %s in %s cannot contain a "/" - ignoring!' % (option, vacuumPipeURL)
                  continue

                elif (option == 'user_data' or option == 'root_image') and '/../' in value:
                  print 'Option %s in %s cannot contain "/../" - ignoring!' % (option, vacuumPipeURL)
                  continue

                elif option == 'user_data' and '/' in value and \
                     not value.startswith('http://') and \
                     not value.startswith('https://'):
                  print 'Option %s in %s cannot contain a "/" unless http(s)://... - ignoring!' % (option, vacuumPipeURL)
                  continue

                elif option == 'root_image' and '/' in value and \
                     not value.startswith('docker://') and \
                     not value.startswith('/cvmfs/') and \
                     not value.startswith('http://') and \
                     not value.startswith('https://'):
                  print 'Option %s in %s cannot contain a "/" unless http(s)://... or /cvmfs/... or docker://... - ignoring!' % (option, vacuumPipeURL)
                  continue

                # if all OK, then can set value as if from configuration files
                parser.set('machinetype ' + machinetypeNamePrefix + '-' + suffix, option, value)

      if printConf:
        print 'Configuration including any machinetypes from Vacuum Pipes:'
        print
        parser.write(sys.stdout)        
        print

      # all other sections are machinetypes (other types of section are ignored)
      for sectionName in parser.sections():

         sectionNameSplit = sectionName.lower().split(None,1)
         
         # For now, can still define these machinetype sections with [vmtype ...] too
         if sectionNameSplit[0] == 'machinetype' or sectionNameSplit[0] == 'vmtype':

             if string.translate(sectionNameSplit[1], None, '0123456789abcdefghijklmnopqrstuvwxyz-') != '':
                 return 'Name of machinetype section [machinetype ' + sectionNameSplit[1] + '] can only contain a-z 0-9 or -'
         
             if sectionNameSplit[0] == 'vmtype':
                 print '[vmtype ...] is deprecated. Please use [machinetype ' + sectionNameSplit[1] + '] instead'
         
             # Start from any factory-wide common values defined in [settings]
             machinetype = machinetypeCommon.copy()
             
             # Now go through the machinetype options, whether from configuration files or vacuum pipes

             # Always set machinetype_path, saved in vacuum pipe processing or default using machinetype name
             try:
               machinetype['machinetype_path'] = parser.get(sectionName, 'machinetype_path').strip()
             except:
               machinetype['machinetype_path'] = '/var/lib/vac/machinetypes/' + sectionNameSplit[1]
                          
             if parser.has_option(sectionName, 'cernvm_signing_dn'):
                 machinetype['cernvm_signing_dn'] = parser.get(sectionName, 'cernvm_signing_dn').strip()
                 print 'cernvm_signing_dn is deprecated - please use image_signing_dn'
             elif parser.has_option(sectionName, 'image_signing_dn'):
                 machinetype['image_signing_dn'] = parser.get(sectionName, 'image_signing_dn').strip()

             if parser.has_option(sectionName, 'target_share'):
                 machinetype['share'] = float(parser.get(sectionName, 'target_share'))
             elif parser.has_option('targetshares', sectionNameSplit[1]):
                 return "Please use a target_shares option within [" + sectionName + "] rather than a separate [targetshares] section. You can still group target shares together or put them in a separate file: see the Admin Guide for details."
             else:
                 machinetype['share'] = 0.0
                                            
             if parser.has_option(sectionName, 'vm_model'):
                 print 'vm_model is deprecated. Please use machine_model in [' + sectionName + '] instead'
                 machinetype['machine_model'] = parser.get(sectionName, 'vm_model')
             elif parser.has_option(sectionName, 'machine_model'):
                 machinetype['machine_model'] = parser.get(sectionName, 'machine_model')
             else:
                 machinetype['machine_model'] = 'cernvm3'
                 
             if machinetype['machine_model'] not in lmModels:
               return 'Machine model %s is not defined!' % machinetype['machine_model']
             
             if parser.has_option(sectionName, 'root_image'):
                 machinetype['root_image'] = parser.get(sectionName, 'root_image')

                 if machinetype['root_image'].startswith('docker://') and \
                    machinetype['machine_model'] not in dcModels:
                   return 'Can only use a docker:// image URI with Docker machine models!'

             if parser.has_option(sectionName, 'root_device'):
               if string.translate(parser.get(sectionName, 'root_device'), None, '0123456789abcdefghijklmnopqrstuvwxyz') != '':
                 print 'root_device can only contain characters a-z 0-9 so skipping machinetype!'
                 continue

               machinetype['root_device'] = parser.get(sectionName, 'root_device')                 
             else:
               machinetype['root_device'] = 'vda'          

             if parser.has_option(sectionName, 'scratch_device'):
               if string.translate(parser.get(sectionName, 'scratch_device'), None, '0123456789abcdefghijklmnopqrstuvwxyz') != '':
                 print 'scratch_device can only contain characters a-z 0-9 so skipping machinetype!'
                 continue
                 
                 machinetype['scratch_device'] = parser.get(sectionName, 'scratch_device')
             else:
                 machinetype['scratch_device'] = 'vdb'

             if parser.has_option(sectionName, 'rootpublickey'):
                 print 'The rootpublickey option in ' + sectionName + ' is deprecated; please use root_public_key in [settings]!'
             elif parser.has_option(sectionName, 'root_public_key'):
                 print 'The root_public_key option in ' + sectionName + ' is deprecated; please use root_public_key in [settings]!'

             if parser.has_option(sectionName, 'user_data'):
                 machinetype['user_data'] = parser.get(sectionName, 'user_data')

             if parser.has_option(sectionName, 'container_command'):
                 machinetype['container_command'] = parser.get(sectionName, 'container_command')
             else:
                 machinetype['container_command'] = '/user_data'

             if parser.has_option(sectionName, 'tmp_binds'):
                 machinetype['tmp_binds'] = set(parser.get(sectionName, 'tmp_binds').strip().split())
                 
             if parser.has_option(sectionName, 'disk_gb_per_processor'):
                 # Size in GB/cpu (1000^3) of disk assigned to machines
                 try:
                   machinetype['disk_gb_per_processor'] = int(parser.get(sectionName, 'disk_gb_per_processor').strip())
                 except:
                   pass

             if parser.has_option(sectionName, 'min_processors'):
                 machinetype['min_processors'] = int(parser.get(sectionName, 'min_processors'))
             else:
                 machinetype['min_processors'] = 1

             if parser.has_option(sectionName, 'max_processors'):
                 machinetype['max_processors'] = int(parser.get(sectionName, 'max_processors'))
             else:             
                 machinetype['max_processors'] = 1               

             if machinetype['max_processors'] < machinetype['min_processors']:
                 return 'max_processors cannot be less than min_processors!'

             if parser.has_option(sectionName, 'log_machineoutputs'):
                 print 'log_machineoutputs has been deprecated: please use machines_dir_days to control this'
             
             if parser.has_option(sectionName, 'machineoutputs_days'):
                 print 'machineoutputs_days is deprecated. Please use machines_dir_days in [' + sectionName + '] instead'
                 machinetype['machines_dir_days'] = float(parser.get(sectionName, 'machineoutputs_days'))
             elif parser.has_option(sectionName, 'machines_dir_days'):
                 machinetype['machines_dir_days'] = float(parser.get(sectionName, 'machines_dir_days'))
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
             
             if parser.has_option(sectionName, 'machinegroup'):
                 machinetype['machinegroup'] = parser.get(sectionName, 'machinegroup')
             elif 'accounting_fqan' in machinetype:
                 machinetype['machinegroup'] = machinetype['accounting_fqan']
             else:
                 machinetype['machinegroup'] = sectionNameSplit[1]
                                       
             if parser.has_option(sectionName, 'cvmfs_repositories'):
               machinetype['cvmfs_repositories'] = set(parser.get(sectionName, 'cvmfs_repositories').split())
             else:
               machinetype['cvmfs_repositories'] = set([])
          
             for (oneOption,oneValue) in parser.items(sectionName):

                 if (oneOption[0:17] == 'user_data_option_') or (oneOption[0:15] == 'user_data_file_'):

                   if string.translate(oneOption, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
                     return 'Name of user_data_option_xxx (' + oneOption + ') must only contain a-z 0-9 and _'
                   else:              
                     machinetype[oneOption] = parser.get(sectionName, oneOption)                
             
             if parser.has_option(sectionName, 'user_data_proxy_cert') or \
                parser.has_option(sectionName, 'user_data_proxy_key') :
               print 'user_data_proxy_cert and user_data_proxy_key are deprecated. Please use user_data_proxy = True and create x509cert.pem and x509key.pem!'
             
             if parser.has_option(sectionName, 'user_data_proxy') and \
                parser.get(sectionName,'user_data_proxy').strip().lower() == 'true':
                 machinetype['user_data_proxy'] = True
             else:
                 machinetype['user_data_proxy'] = False
             
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

def setCgroupFsRoots():
      global cpuCgroupFsRoot, memoryCgroupFsRoot
      
      cpuCgroupFsRoot    = None
      memoryCgroupFsRoot = None
        
      try:
        f = open('/proc/mounts', 'r')
      except:
        vac.vacutils.logLine('Failed to open /proc/mounts')
      
      for line in f:
         cgroup, path = line.split()[:2]
         
         # CPU cgroup line in /proc/mounts might look like this, with cpuacct listed too:
         # cgroup /sys/fs/cgroup/cpu,cpuacct cgroup rw,nosuid,nodev,noexec,relatime,cpuacct,cpu 0 0 
         if cgroup == 'cgroup':
           if 'cpu' in path.split('/')[-1].split(','):
             cpuCgroupFsRoot = path
           elif 'memory' in path.split('/')[-1].split(','):
             memoryCgroupFsRoot = path

      f.close()
      
      if cpuCgroupFsRoot:
        vac.vacutils.logLine('Filesystem root of the CPU cgroup is ' + cpuCgroupFsRoot)
      else:
        vac.vacutils.logLine('Could not determine filesystem root of the CPU cgroup!')

      if memoryCgroupFsRoot:
        vac.vacutils.logLine('Filesystem root of the memory cgroup is ' + memoryCgroupFsRoot)
      else:
        vac.vacutils.logLine('Could not determine filesystem root of the memory cgroup!')

def getProcessCpuCgroupPath(pid):
      if not cpuCgroupFsRoot:
        raise VacError('cpuCgroupFsRoot is not set!')
      
      try:
        f = open('/proc/%d/cgroup' % pid, 'r')
      except:
        raise VacError('No cgroup file for PID %d!' % pid)
        
      for line in f:
        n,subsystems,path = line.strip().split(':')
        
        if 'cpu' in subsystems.split(','):
          f.close()
          return cpuCgroupFsRoot + path
          
      f.close()       
      
      raise VacError('No CPU cgroup for PID %d!' % pid)  
       
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
     if int(open('/proc/sys/net/core/wmem_max', 'r').readline().strip()) < udpBufferSize:
       open('/proc/sys/net/core/wmem_max', 'w').write(str(udpBufferSize) + '\n')
   except:
     pass

   try:
     sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, udpBufferSize)
   except:
     vac.vacutils.logLine('Failed setting RCVBUF to %d' % udpBufferSize)
     
   try:
     sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, udpBufferSize)
   except:
     vac.vacutils.logLine('Failed setting SNDBUF to %d' % udpBufferSize)
     
def canonicalFQDN(hostName):

   if hostName == '.':
     # . is replaced with local hostname
     return os.uname()[1]

   if '.' in hostName:
     # Otherwise assume ok if already contains '.'
     return hostName
     
   try:
     # Try to get DNS domain from current host's FQDN
     return hostName + '.' + os.uname()[1].split('.',1)[1]
   except:
     # If failed, then just return what we were given
     return hostName

def killZombieVMs():
   # Look for VMs which are not properly associated with
   # logical machine slots and kill them
   
   conn = libvirt.open(None)
   if conn == None:
     vac.vacutils.logLine('Failed to open connection to the hypervisor')
     raise

   for ordinal in xrange(numMachineSlots):
      name = nameFromOrdinal(ordinal)
      
      try:
        createdStr, machinetypeName, machineModel = open('/var/lib/vac/slots/' + name,'r').read().split()
      except:
        createdStr      = None
        machinetypeName = None
        machineModel    = None
        uuidStr         = None
      else:
        try: 
          # Can't use self.machinesDir()
          uuidStr = open('/var/lib/vac/machines/' + createdStr + '_' + machinetypeName + '_' + name + '/jobfeatures/job_id', 'r').read().strip()
        except:
          uuidStr = None

      try:
        dom = conn.lookupByName(name)
      except:
        # Not running so can continue
        continue

      killZombie = False
      
      if machineModel not in vmModels:
        # We think a non-VM should be running here
        vac.vacutils.logLine('VM still running alongside %s LM in slot %s, killing zombie' % (self.machineModel, self.name))
        killZombie = True

      if uuidStr != dom.UUIDString():
        # Doesn't match slot's UUID
        vac.vacutils.logLine('UUID mismatch: %s (job_id) != %s (dom) for LM %s, killing zombie' % (str(uuidStr), dom.UUIDString(), name))
        killZombie = True

      if not createdStr or not os.path.isdir('/var/lib/vac/machines/' + createdStr + '_' + machinetypeName + '_' + name):
        # Our files say otherwise
        vac.vacutils.logLine('No created time (or missing machines dir), killing zombie')
        killZombie = True

      if killZombie:
        try:
          dom.shutdown()
        except Exception as e:
          vac.vacutils.logLine('Failed to shutdown %s (%s)' % (name, str(e)))
        else:
          # 30s delay for any ACPI handler in the VM
          time.sleep(30.0)

        try:
          dom.destroy()
        except Exception as e:
          vac.vacutils.logLine('Failed to destroy %s (%s)' % (name, str(e)))
             
   conn.close()
   
def killZombieDCs():
   # Look for Docker Container processes which are not properly associated with
   # logical machine slots and kill them
   
   try:
     containers = dockerPsCommand()
   except Exception as e:
     vac.vacutils.logLine('Failed to get list of Docker zombie candidates (%s)' % str(e))
     return

   for name in containers: 
     # Look at this container looking for a mismatch. Unless we continue, remove the container
     
     try:
       createdStr, machinetypeName, machineModel = open('/var/lib/vac/slots/' + name,'r').read().split()
     except:
       # The corresponding slot isn't defined. Container is a zombie!
       pass
     else:
       if machineModel in dcModels:
         # This slot IS a Docker container. So may not be a zombie!
         try:       
           uuidStr = open('/var/lib/vac/machines/%s_%s_%s/jobfeatures/job_id' % (createdStr, machinetypeName, name),'r').read().strip()
         except Exception as e:
           # But no UUID/ID defined for the slot's container. A zombie!
           pass
         else:
           if uuidStr == containers[name]['id']:
             # UUID = ID, so in this one case, NOT a zombie!
             continue

     # We've fallen through one way or another. So a zombie!
     vac.vacutils.logLine('Removing zombie Docker container %s' % name)
     dockerRmCommand(name)

def killZombieSCs():
   # Look for Singularity Container processes which are not properly 
   # associated with logical machine slots and kill them;
   # And remove Vac Singularity cgroups which have no processes

   if singularityUser:

     # First compose a list of expected running Singularity Container CPU cgroups
     singularityCpuCgroupPaths = []
   
     for ordinal in xrange(numMachineSlots):
       name = nameFromOrdinal(ordinal)
   
       try:
         createdStr, machinetypeName, machineModel = open('/var/lib/vac/slots/' + name,'r').read().split()
       except:
         continue
         
       if machineModel not in scModels:
         # If slot not meant to be an SC then ignore
         continue

       try: 
         uuidStr = open('/var/lib/vac/machines/' + createdStr + '_' + machinetypeName + '_' + name + '/jobfeatures/job_id', 'r').read().strip()
       except:
         uuidStr = None
         
       try:
         finished = int(os.stat('/var/lib/vac/machines/' + createdStr + '_' + machinetypeName + '_' + name + '/finished').st_ctime)
       except:
         finished = None
                  
       if uuidStr and finished is None:
         # Only add cgroup if the SC hasn't finished
         singularityCpuCgroupPaths.append(cpuCgroupFsRoot + '/vac/singularity-' + uuidStr)

     vac.vacutils.logLine('Running singularity CPU cgroup paths: ' + str(singularityCpuCgroupPaths))

     # Now find all the processes of singularityUser and check if valid (in a Vac SC CPU cgroup)
     for pid in os.listdir('/proc'):
     
       if not pid.isdigit():
         continue
       
       try:
         uid = int(os.stat('/proc/' + pid).st_uid)
       except:
         # Failed to get process owner ID. Process gone?
         continue
         
       if uid != singularityUid:
         # Ignore processes unless they are owned by singularityUser
         continue
       
       try:
         cpuCgroupPath = getProcessCpuCgroupPath(int(pid))
       except:
         # Failed to get process cgroup path? Process gone?
         continue

       if cpuCgroupPath not in singularityCpuCgroupPaths:
         # Process group ID does not correspond to any valid Vac Singularity Container CPU cgroup!
         vac.vacutils.logLine('Kill zombie process %s of Singularity User (%s)' % (pid, singularityUser))
         os.kill(int(pid), signal.SIGKILL)
   
   # Remove unused cgroups

   if os.path.exists(cpuCgroupFsRoot + '/vac'):
     for i in os.listdir(cpuCgroupFsRoot + '/vac'):
       if i.startswith('singularity-'):
         if len(open(cpuCgroupFsRoot + '/vac/' + i + '/cgroup.procs').read()) == 0:
           vac.vacutils.logLine('Remove unused cgroup ' + cpuCgroupFsRoot + '/vac/' + i)
           os.rmdir(cpuCgroupFsRoot + '/vac/' + i)       

   if os.path.exists(memoryCgroupFsRoot + '/vac'):
     for i in os.listdir(memoryCgroupFsRoot + '/vac'):
       if i.startswith('singularity-'):
         if len(open(memoryCgroupFsRoot + '/vac/' + i + '/cgroup.procs').read()) == 0:
           vac.vacutils.logLine('Remove unused cgroup ' + memoryCgroupFsRoot + '/vac/' + i)
           os.rmdir(memoryCgroupFsRoot + '/vac/' + i)       

class VacState:
   unknown, shutdown, starting, running, paused, zombie = ('Unknown', 'Shut down', 'Starting', 'Running', 'Paused', 'Zombie')

class VacSlot:
   # This class represents logical machine slots

   def __init__(self, ordinal, forResponder = False):
      self.ordinal             = ordinal
      self.name                = nameFromOrdinal(ordinal)
      self.ip                  = None
      self.state               = VacState.unknown
      self.started             = None
      self.finished            = None
      self.heartbeat           = None
      self.joboutputsHeartbeat = None
      self.cpuSeconds          = 0
      self.cpuPercentage       = 0
      self.processors          = 0
      self.mb                  = 0
      self.hs06                = None
      self.accountingFqan      = None
      self.shutdownMessage     = None
      self.shutdownMessageTime = None
      self.created             = None
      self.machinetypeName     = None
      self.machineModel        = None

      try:
        createdStr, self.machinetypeName, self.machineModel = open('/var/lib/vac/slots/' + self.name,'r').read().split()
        self.created = int(createdStr)
      except:
        pass

      try:
        self.cvmfsRepositories = machinetypes[self.machinetypeName]['cvmfs_repositories']
      except:
        self.cvmfsRepositories = ''

      try: 
        self.uuidStr = open(self.machinesDir() + '/jobfeatures/job_id', 'r').read().strip()
      except:
        self.uuidStr = None
                                              
      if not self.created or not os.path.isdir(self.machinesDir()):
        # if slot not properly set up or if machines directory is missing then stop now
        self.state           = VacState.shutdown
        self.machinetypeName = None
        self.created         = None
        return

      try:
        self.started = int(os.stat(self.machinesDir() + '/started').st_ctime)
        # if created and started, state must be running or shutdown
      except:
        # if created but not yet started, then state is starting
        self.started = None
        self.state = VacState.starting

      try:
        self.ip = open(self.machinesDir() + '/ip', 'r').read().strip()
      except:
        pass

      try:
        self.accountingFqan = open(self.machinesDir() + '/accounting_fqan', 'r').read().strip()
      except:
        self.accountingFqan = None

      try:
        self.finished = int(os.stat(self.machinesDir() + '/finished').st_ctime)
      except:
        self.finished = None
      else:
        self.state = VacState.shutdown

      if self.started and not self.finished:
        self.state = VacState.running

      try:
        self.heartbeat = int(os.stat(self.machinesDir() + '/heartbeat').st_ctime)
      except:
        self.heartbeat = None

      try: 
        self.shutdownTime = int(open(self.machinesDir() + '/jobfeatures/shutdowntime_job', 'r').read().strip())
      except:
        self.shutdownTime = None

      try:
        self.joboutputsHeartbeat = int(os.stat(self.machinesDir() + '/joboutputs/' + 
                                               machinetypes[self.machinetypeName]['heartbeat_file']).st_mtime)
      except:
        self.joboutputsHeartbeat = None

      try: 
        self.processors = int(open(self.machinesDir() + '/jobfeatures/allocated_cpu', 'r').read().strip())
      except:
        pass
      
      try: 
        self.hs06 = float(open(self.machinesDir() + '/machinefeatures/hs06', 'r').read().strip())
      except:
        pass
      
      try: 
        self.mb = (int(open(self.machinesDir() + '/jobfeatures/max_rss_bytes', 'r').read().strip()) / 1048576)
      except:
        self.mb = mbPerProcessor * self.processors
      
      try:
        # this is written by Vac as it monitors the logical machine
        oneLine = open(self.machinesDir() + '/heartbeat', 'r').readline()
                                    
        self.cpuSeconds = int(oneLine.split(' ')[0])
        try:
          self.cpuPercentage = float(oneLine.split(' ')[1])
        except:
          self.cpuPercentage = 0
                    
      except:
        self.cpuSeconds    = 0
        self.cpuPercentage = 0
 
      # Virtual Machine models
      if not forResponder and self.machineModel in vmModels:
        dom      = None
        domState = None

        conn = libvirt.open(None)
        if conn == None:
          vac.vacutils.logLine('Failed to open connection to the hypervisor')
          raise

        try:
          dom = conn.lookupByName(self.name)
          domState = dom.info()[0]
        except:
          pass

        conn.close()

        if dom:
          if domState != libvirt.VIR_DOMAIN_RUNNING and domState != libvirt.VIR_DOMAIN_BLOCKED:
            # If domain exists, but not Running/Blocked, then say Paused
            self.state = VacState.paused
            vac.vacutils.logLine('!!! libvirt state is ' + str(domState) + ', setting VacState.paused !!!')

          try:
            # Overwrite with better estimate from hypervisor
            self.cpuSeconds = int(dom.info()[4] / 1000000000.0)
          except:
            pass

        else:
          # Actually, we're shutdown since VM not really running
          self.state = VacState.shutdown

      # Singularity Container models
      if not forResponder and self.machineModel in scModels:

        try:
          pid = int(open(self.machinesDir() + '/pid', 'r').read().strip())
        except:
          pid = None
          uid = None
        else:
          try:
            uid = int(os.stat('/proc/' + str(pid)).st_uid)
          except:
            uid = None

        if pid is None or uid is None or uid != singularityUid:
          # Actually, we're shutdown since SC head process is not really running
          self.state = VacState.shutdown
        else:
          try:
            self.cpuSeconds = int(open(cpuCgroupFsRoot + '/vac/singularity-' + self.uuidStr + '/cpuacct.usage', 'r').read()) / 1000000000
          except:
            pass

      # Docker Container models
      if not forResponder and self.machineModel in dcModels:
      
        id    = None          
        state = None

        try:
          containers = dockerPsCommand()
        except:
          vac.vacutils.logLine('Failed to get list of defined Docker containers')
        else:
          if self.name in containers:
            id = containers[self.name]['id']
            status = containers[self.name]['status']
            
        if id is None or status != 'Up':
          self.state = VacState.shutdown
        else:
          try:
            self.cpuSeconds = int(open(getProcessCpuCgroupPath(containers[self.name]['pid']) + '/cpuacct.usage', 'r').read()) / 1000000000
          except:
            pass

      if self.state == VacState.shutdown:
        try:
          self.shutdownMessage = open(self.machinesDir() + '/joboutputs/shutdown_message', 'r').read().strip()
          self.shutdownMessageTime = int(os.stat(self.machinesDir() + '/joboutputs/shutdown_message').st_ctime)
        except:
          pass
      
   def machinesDir(self):
      return '/var/lib/vac/machines/' + str(self.created) + '_' + self.machinetypeName + '_' + self.name

   def createHeartbeatFile(self):
      self.heartbeat = int(time.time())
      
      try:
        f = open(self.machinesDir() + '/heartbeat', 'r')
        lastCpuSeconds = int(f.readline().split(' ')[0])
        f.close()
        
        lastHeartbeat = int(os.stat(self.machinesDir() + '/heartbeat').st_ctime)
                                    
        cpuPercentage = 100.0 * float(self.cpuSeconds - lastCpuSeconds) / (self.heartbeat - lastHeartbeat)
        heartbeatLine = str(self.cpuSeconds) + (" %.1f" % cpuPercentage)
      except:
        heartbeatLine = str(self.cpuSeconds)

      try:
        vac.vacutils.createFile(self.machinesDir() + '/heartbeat', heartbeatLine + '\n', stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
      except:
        pass
                                  
   def createFinishedFile(self):
   
      if os.path.isdir(self.machinesDir()):
        try:
          vac.vacutils.createFile(self.machinesDir() + '/finished',
                                '',
                                stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')

        except:
          vac.vacutils.logLine('Failed creating ' + self.machinesDir() + '/finished')

      # Update the file for this machinetype in the finishes directory, about the most recently created but already finished machine

      finishedFilesList = glob.glob('/var/lib/vac/machines/*_' + self.machinetypeName + '_*/finished')
     
      if finishedFilesList:
        finishedFilesList.sort()

        try:
          vac.vacutils.createFile('/var/lib/vac/finishes/' + self.machinetypeName,
                                  finishedFilesList[-1].split('/')[-2].replace('_',' '),
                                  stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
        except:
          vac.vacutils.logLine('Failed creating /var/lib/vac/finishes/' + self.machinetypeName)

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
        tmpGocdbSitename = '.'.join(spaceName.split('.')[1:]) if '.' in spaceName else spaceName

      if self.hs06:
        hs06 = self.hs06 / self.processors
      else:
        hs06 = 1.0

      mesg = ('APEL-individual-job-message: v0.3\n' + 
              'Site: ' + tmpGocdbSitename + '\n' +
              'SubmitHost: ' + spaceName + '/vac-' + os.uname()[1] + '\n' +
              'LocalJobId: ' + str(self.uuidStr) + '\n' +
              'LocalUserId: ' + os.uname()[1] + '\n' +
              'Queue: ' + self.machinetypeName + '\n' +
              'GlobalUserName: ' + userDN + '\n' +
              userFQANField +
              'WallDuration: ' + str(self.heartbeat - self.started) + '\n' +
              'CpuDuration: ' + str(self.cpuSeconds) + '\n' +
              'Processors: ' + str(self.processors) + '\n' +
              'NodeCount: 1\n' +
              'InfrastructureDescription: APEL-VAC\n' +
              'InfrastructureType: grid\n' +
              'StartTime: ' + str(self.started) + '\n' +
              'EndTime: ' + str(self.heartbeat) + '\n' +
              'MemoryReal: ' + str(self.mb * 1024) + '\n' +
              'MemoryVirtual: ' + str(self.mb * 1024) + '\n' +
              'ServiceLevelType: HEPSPEC\n' +
              'ServiceLevel: ' + str(hs06) + '\n' +
              '%%\n')
                          
      fileName = time.strftime('%H%M%S', nowTime) + (str(time.time() % 1) + '00000000')[2:10] 

      try:
        vac.vacutils.createFile(time.strftime('/var/lib/vac/apel-archive/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
      except:
        vac.vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vac/apel-archive/%Y%m%d/', nowTime) + fileName)
        return

      if gocdbSitename and self.hs06:
        # We only write the outgoing copy if gocdb_sitename and HS06 are explicitly given
        try:
          vac.vacutils.createFile(time.strftime('/var/lib/vac/apel-outgoing/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
        except:
          vac.vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vac/apel-outgoing/%Y%m%d/', nowTime) + fileName)
          return

   def sendVacMon(self):
      # Send VacMon machine_status message(s) about a VM that has finished
      if not vacmons or self.state != VacState.shutdown or not self.started or not self.heartbeat:
        return

      machineMessage = makeMachineResponse('0', self.ordinal, clientName = 'vacd-factory')
      sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

      for vacmonHostPort in vacmons:
        (vacmonHost, vacmonPort) = vacmonHostPort.split(':')
        sock.sendto(machineMessage, (vacmonHost,int(vacmonPort)))

      sock.close()

   def makeOpenStackData(self):
   
      if rootPublicKeyFile:
        try:
          publicKey = open(rootPublicKeyFile, 'r').read()
        except:
          vac.vacutils.logLine('Failed to read ' + rootPublicKeyFile + ' so no ssh access to VMs!')
        else:
          try:
            open(self.machinesDir() + '/root_public_key','w').write(publicKey)
          except:
            raise VacError('Failed to create root_public_key')

   def makeMJF(self):
      os.makedirs(self.machinesDir() + '/machinefeatures', stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      os.makedirs(self.machinesDir() + '/jobfeatures',     stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      os.makedirs(self.machinesDir() + '/joboutputs',      stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

      # HEPSPEC06 per virtual machine
      if hs06PerProcessor:
        vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/hs06',
                 str(hs06PerProcessor * self.processors), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Easy in 2016 MJF
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/total_cpu',
                 str(self.processors), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Deprecated. We don't know the physical vs logical cores distinction here so we just use cpu
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/phys_cores',
                 str(self.processors), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Deprecated. Again just use cpu
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/log_cores',
                 str(self.processors), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Deprecated. Tell them they have the whole VM to themselves; they are in the only jobslot here
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/jobslots',
                '1', stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
      
      cpuLimitSecs = self.shutdownTime - int(time.time())
      if (cpuLimitSecs < 0):
        cpuLimitSecs = 0

      # calculate the absolute shutdown time for the VM, as a machine
      vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/shutdowntime',
                 str(self.shutdownTime),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # additional machinefeatures options defined in configuration
      if machinefeaturesOptions:
        for oneOption,oneValue in machinefeaturesOptions.iteritems():
          vac.vacutils.createFile(self.machinesDir() + '/machinefeatures/' + oneOption,
                                  oneValue,
                                  stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Jobfeatures
      
      # Calculate the absolute shutdown time for the VM, as a job
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/shutdowntime_job',
                 str(self.shutdownTime),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Deprecated. We don't do this, so just say 1.0 for cpu factor
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/cpufactor_lrms',
                 '1.0', stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Deprecated. For the scaled cpu limit, we use the wallclock seconds multiple by the cpu
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/cpu_limit_secs_lrms',
                 str(cpuLimitSecs * self.processors),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')


      # For the cpu limit, we use the wallclock seconds multiple by the cpu
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/cpu_limit_secs',
                 str(cpuLimitSecs * self.processors),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Deprecated. For the scaled wallclock limit, we use the wallclock seconds without factoring in cpu
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/wall_limit_secs_lrms',
                 str(cpuLimitSecs),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # For the wallclock limit, we use the wallclock seconds without factoring in cpu
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/wall_limit_secs',
                 str(cpuLimitSecs),
                 stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # We are about to start the VM now
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/jobstart_secs',
                 str(int(time.time())), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')



      # Job=VM so per-job HEPSPEC06 is same as hs06
      if hs06PerProcessor:
        vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/hs06_job',
                  str(hs06PerProcessor * self.processors), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # mbPerProcessor is in units of 1024^2 bytes
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/max_rss_bytes',
                 str(mbPerProcessor * self.processors * 1048576), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Deprecated. mbPerProcessor is in units of 1024^2 bytes, whereas old jobfeatures wants 1000^2!!!
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/mem_limit_MB',
                 str((mbPerProcessor * self.processors * 1048576) / 1000000), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')


      # cpuPerMachine again
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/allocated_cpu',
                 str(self.processors) + '\n', stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # Deprecated. cpuPerMachine again
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/allocated_CPU',
                 str(self.processors) + '\n', stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      # We do not know max_swap_bytes or scratch_limit_bytes so ignore them

   def setupUserDataContents(self):
   
      if machinetypes[self.machinetypeName]['root_image'].startswith('http://') or \
         machinetypes[self.machinetypeName]['root_image'].startswith('https://'):
       rootImageURL = machinetypes[self.machinetypeName]['root_image']
      else:
       rootImageURL = None

      # Set the MJFJO paths for the different machine models       
      if self.machineModel in vmModels: 
        machinefeaturesURL = 'http://' + mjfAddress + '/machinefeatures'
        jobfeaturesURL     = 'http://' + mjfAddress + '/jobfeatures'
        joboutputsURL      = 'http://' + mjfAddress + '/joboutputs'
      elif self.machineModel in dcModels:
        machinefeaturesURL = '/etc/machinefeatures'
        jobfeaturesURL     = '/etc/jobfeatures'
        joboutputsURL      = '/var/spool/joboutputs'
      elif self.machineModel in scModels:
        machinefeaturesURL = '/tmp/machinefeatures'
        jobfeaturesURL     = '/tmp/jobfeatures'
        joboutputsURL      = '/tmp/joboutputs'
      else:
        machinefeaturesURL = None
        jobfeaturesURL     = None
        joboutputsURL      = None
    
      try:
        userDataContents = vac.vacutils.createUserData(
                                               shutdownTime       = self.shutdownTime,
                                               machinetypePath	  = machinetypes[self.machinetypeName]['machinetype_path'],
                                               options		  = machinetypes[self.machinetypeName],
                                               versionString	  = 'Vac ' + vacVersion,
                                               spaceName	  = spaceName, 
                                               machinetypeName	  = self.machinetypeName, 
                                               userDataPath	  = machinetypes[self.machinetypeName]['user_data'], 
                                               hostName		  = self.name, 
                                               uuidStr		  = self.uuidStr,
                                               machinefeaturesURL = machinefeaturesURL,
                                               jobfeaturesURL     = jobfeaturesURL,
                                               joboutputsURL      = joboutputsURL,
                                               rootImageURL       = rootImageURL )
      except Exception as e:
        raise VacError('Failed to read ' + machinetypes[self.machinetypeName]['user_data'] + ' (' + str(e) + ')')

      try:
        o = open(self.machinesDir() + '/user_data', 'w')
        o.write(userDataContents)
        o.close()
      except:
        raise VacError('Failed writing to ' + self.machinesDir() + '/user_data')

   def destroy(self, shutdownMessage = None):
      # Destroy the logical machine in this slot
   
      if self.machineModel in vmModels:
        self.destroyVM()
      elif self.machineModel in dcModels:
        self.destroyDC()
      elif self.machineModel in scModels:
        self.destroySC()
      else:
        vac.vacutils.logLine('Machinemodel %s is not supported - cleaning up anyway' % self.machineModel)

      # Common finalization

      self.state = VacState.shutdown
      self.removeLogicalVolume()

      if shutdownMessage and not os.path.exists(self.machinesDir() + '/joboutputs/shutdown_message'):
        try:
          open(self.machinesDir() + '/joboutputs/shutdown_message', 'w').write(shutdownMessage)
        except:
          pass

   def create(self, machinetypeName, cpus, machineShutdownTime):
      # Create a logical machine in this slot 

      self.machineModel    = machinetypes[machinetypeName]['machine_model']
      self.processors      = cpus
      self.created         = int(time.time())
      self.shutdownTime    = machineShutdownTime
      self.machinetypeName = machinetypeName
      self.uuidStr         = None

      os.makedirs(self.machinesDir(), stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

      try:
        os.makedirs('/var/lib/vac/slots', 
                  stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      except:
        pass

      vac.vacutils.createFile('/var/lib/vac/slots/' + self.name,
                              str(self.created) + ' ' + self.machinetypeName + ' ' + self.machineModel,
                              stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')

      if 'accounting_fqan' in machinetypes[machinetypeName]:
        vac.vacutils.createFile(self.machinesDir() + '/accounting_fqan', machinetypes[machinetypeName]['accounting_fqan'],
                              stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')

      try:
        self.makeMJF()
      except Exception as e:
        raise VacError('Failed making MJF files (' + str(e) + ')')
        

      if 'user_data' in machinetypes[self.machinetypeName]:
        try:
          self.setupUserDataContents()
        except Exception as e:
          raise VacError('Failed to create user_data (' + str(e) + ')')

      try:
        self.makeOpenStackData()
      except Exception as e:
        raise VacError('Failed making OpenStack meta_data (' + str(e) + ')')

      # Create a logical machine with the appropriate model in this slot
      if self.machineModel in vmModels:
        self.createVM()
      elif self.machineModel in dcModels:
        self.createDC()
      elif self.machineModel in scModels:
        self.createSC()
      else:
        raise VacError('machineModel %s is not supported' % self.machineModel)

      vac.vacutils.createFile(self.machinesDir() + '/started',
                  str(int(time.time())) + '\n', stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')
      
      vac.vacutils.createFile(self.machinesDir() + '/heartbeat',
                 '0.0 0.0\n', stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vac/tmp')

   def removeLogicalVolume(self):
   
      if volumeGroup and os.path.exists('/dev/' + str(volumeGroup) + '/' + self.name):
      
        # First try to unmount the logical volume in case used for Singularity
        try:
          # Kill any processes still using the filesystem
          os.system('/usr/sbin/fuser --kill --mount /dev/' + str(volumeGroup) + '/' + self.name)
          # Unmount the filesystem itself
          os.system('/usr/bin/umount /dev/' + str(volumeGroup) + '/' + self.name)
        except:
          pass
        else:
          vac.vacutils.logLine('Unmount logical volume /dev/' + volumeGroup + '/' + self.name)

        # Now remove the logical volume itself
        vac.vacutils.logLine('Remove logical volume /dev/' + volumeGroup + '/' + self.name)
        os.system('LVM_SUPPRESS_FD_WARNINGS=1 /sbin/lvremove -f ' + volumeGroup + '/' + self.name + ' 2>&1')

   def createLogicalVolume(self):

     # Always remove any leftover volume of the same name
     self.removeLogicalVolume()

     if 'disk_gb_per_processor' in machinetypes[self.machinetypeName] and \
          ((gbDiskPerProcessor is None) or (machinetypes[self.machinetypeName]['disk_gb_per_processor'] <= gbDiskPerProcessor)):
       gbDiskPerProcessorTmp = machinetypes[self.machinetypeName]['disk_gb_per_processor']
     else:
       gbDiskPerProcessorTmp = gbDiskPerProcessor

     try:
       vgsResult = measureVolumeGroup(volumeGroup)
       vgTotalBytes = int(vgsResult[0])
       vgExtentBytes = int(vgsResult[1])
     except Exception as e:
       raise VacError('Failed to measure size of volume group ' + volumeGroup + ' - missing?')

     try:
       f = os.popen('LVM_SUPPRESS_FD_WARNINGS=1 /sbin/lvs --noheadings --units B --nosuffix --options lv_name,lv_size ' + volumeGroup, 'r')
     except Exception as e:
       raise VacError('Measuring size of logical volumes in ' + volumeGroup + ' fails with ' + str(e))

     vgVacBytes = 0
     vgNonVacBytes = 0
     nameParts = os.uname()[1].split('.',1)
     domainRegex = nameParts[1].replace('.','\.')

     while True:
       try:
        name,sizeStr = f.readline().strip().split()
        size = int(sizeStr)
       except:
        break

       if re.search('^' + nameParts[0] + '-[0-9][0-9]\.' + domainRegex + '$', name) is None:
        vgNonVacBytes += size
       else:
        vgVacBytes += size

     f.close()

     vac.vacutils.logLine('Volume group ' + volumeGroup + ' has ' + str(vgVacBytes) + ' bytes used by Vac and ' + str(vgNonVacBytes) + 
                          ' bytes by others, out of ' + str(vgTotalBytes) + ' bytes in total. The extent size is ' + str(vgExtentBytes) + ' bytes.')

     # Now try to create logical volume
     vac.vacutils.logLine('Trying to create logical volume for ' + self.name + ' in ' + volumeGroup)

     if gbDiskPerProcessorTmp:
       # Fixed size has been given in configuration. Round down to match extent size.
       sizeToCreate = ((gbDiskPerProcessorTmp * self.processors * 1000000000) / vgExtentBytes) * vgExtentBytes
     else:
       # Not given, so calculate. Round down to match extent size.
       sizeToCreate = ((self.processors * (vgTotalBytes - vgNonVacBytes) / numProcessors) / vgExtentBytes) * vgExtentBytes
     
     # Option -y means we wipe existing signatures etc
     os.system('LVM_SUPPRESS_FD_WARNINGS=1 /sbin/lvcreate --yes --name ' + self.name + ' -L ' + str(sizeToCreate) + 'B ' + volumeGroup + ' 2>&1')

     try:
       if not stat.S_ISBLK(os.stat('/dev/' + volumeGroup + '/' + self.name).st_mode):
         raise VacError('Failing due to /dev/' + volumeGroup + '/' + self.name + ' not a block device')
     except:
         raise VacError('Failing due to /dev/' + volumeGroup + '/' + self.name + ' not existing')


   def createVM(self):
      # Create Virtual Machine instance in this logical machine slot
   
      self.ip = natPrefix + str(self.ordinal)
      vac.vacutils.createFile(self.machinesDir() + '/ip',
                              self.ip, stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      ipBytes = self.ip.split('.')
      mac     = '56:4D:%02X:%02X:%02X:%02X' % (int(ipBytes[0]), int(ipBytes[1]), int(ipBytes[2]), int(ipBytes[3]))

      vac.vacutils.logLine('Using IP=' + self.ip + ' MAC=' + mac + ' when creating ' + self.name)

      scratch_disk_xml = ""
      cernvm_cdrom_xml = ""
      self.uuidStr     = str(uuid.uuid4())
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/job_id',
                              self.uuidStr, stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')

      if self.machineModel == 'vm-raw':
        # non-CernVM VM model
      
        if machinetypes[self.machinetypeName]['root_image'][0:7] == 'http://' or machinetypes[self.machinetypeName]['root_image'][0:8] == 'https://':
          try:
            rawFileName = vac.vacutils.getRemoteRootImage(machinetypes[self.machinetypeName]['root_image'], '/var/lib/vac/imagecache', '/var/lib/vac/tmp', 'Vac ' + vacVersion)
          except Exception as e:
            raise VacError('Failed fetching root_image ' + machinetypes[self.machinetypeName]['root_image'] + ' (' + str(e) + ')')
        elif machinetypes[self.machinetypeName]['root_image'][0] == '/':
           rawFileName = machinetypes[self.machinetypeName]['root_image']
        else:
           rawFileName = machinetypes[self.machinetypeName]['machinetype_path'] + '/files/' + machinetypes[self.machinetypeName]['root_image']

        if 'cernvm_signing_dn' in machinetypes[self.machinetypeName]:
          cernvmDict = vac.vacutils.getCernvmImageData(rawFileName)
          if cernvmDict['verified'] == False:
            raise VacError('Failed to verify signature/cert for ' + rawFileName)
          elif re.search(machinetypes[self.machinetypeName]['cernvm_signing_dn'],  cernvmDict['dn']) is None:
            raise VacError('Signing DN ' + cernvmDict['dn'] + ' does not match cernvm_signing_dn = ' + machinetypes[self.machinetypeName]['cernvm_signing_dn'])
          else:
            vac.vacutils.logLine('Verified image signed by ' + cernvmDict['dn'])

        fTmp, rootDiskFileName = tempfile.mkstemp(prefix = 'root.disk.', dir = '/var/lib/vac/tmp')

        # Make a small QEMU qcow2 disk for this instance,  backed by the full image stored elsewhere
        if os.system('qemu-img create -b ' + rawFileName + ' -f qcow2 ' + rootDiskFileName + ' >/dev/null') != 0:
          raise VacError('Creation of copy-on-write disk image fails!')

        root_disk_xml = """<disk type='file' device='disk'>
                           <driver name='qemu' type='qcow2' cache='unsafe' error_policy='report' />
                           <source file='""" + rootDiskFileName + """' />
                           <target dev='""" + machinetypes[self.machinetypeName]['root_device'] + """' 
                            bus='""" + ("virtio" if "vd" in machinetypes[self.machinetypeName]['root_device'] else "ide") + """'/>
                           </disk>"""

        # For vm-raw, maybe we have logical volume to use as scratch too?
        if volumeGroup and measureVolumeGroup(volumeGroup):
          try:
            self.createLogicalVolume()
          except Exception as e:
            raise VacError('Failed to create required logical volume: ' + str(e))

          scratch_disk_xml = ("<disk type='block' device='disk'>\n" +
                              " <driver name='qemu' type='raw' error_policy='report' cache='unsafe'/>\n" +
                              " <source dev='/dev/" + volumeGroup + "/" + self.name  + "'/>\n" +
                              " <target dev='" + machinetypes[self.machinetypeName]['scratch_device'] + 
                              "' bus='" + ("virtio" if "vd" in machinetypes[self.machinetypeName]['scratch_device'] else "ide") + "'/>\n</disk>")
      
      elif self.machineModel == 'cernvm3':
        # CernVM VM model
            
        # For cernvm3 always need to set up the ISO boot image
        if machinetypes[self.machinetypeName]['root_image'][0:7] == 'http://' or machinetypes[self.machinetypeName]['root_image'][0:8] == 'https://':
            try:
              cernvmCdrom = vac.vacutils.getRemoteRootImage(machinetypes[self.machinetypeName]['root_image'], '/var/lib/vac/imagecache', '/var/lib/vac/tmp', 'Vac ' + vacVersion)
            except Exception as e:
              raise VacError(str(e))
        elif machinetypes[self.machinetypeName]['root_image'][0] == '/':
            cernvmCdrom = machinetypes[self.machinetypeName]['root_image']
        else:
            cernvmCdrom = machinetypes[self.machinetypeName]['root_image'] + '/files/' + machinetypes[self.machinetypeName]['root_image']

        if 'cernvm_signing_dn' in machinetypes[self.machinetypeName]:
            cernvmDict = vac.vacutils.getCernvmImageData(cernvmCdrom)
            if cernvmDict['verified'] == False:
              raise VacError('Failed to verify signature/cert for ' + cernvmCdrom)
            elif re.search(machinetypes[self.machinetypeName]['cernvm_signing_dn'],  cernvmDict['dn']) is None:
              raise VacError('Signing DN ' + cernvmDict['dn'] + ' does not match cernvm_signing_dn = ' + machinetypes[self.machinetypeName]['cernvm_signing_dn'])
            else:
              vac.vacutils.logLine('Verified image signed by ' + cernvmDict['dn'])
      
        cernvm_cdrom_xml = ("<disk type='file' device='cdrom'>\n" +
                            " <driver name='qemu' type='raw' error_policy='report' cache='unsafe'/>\n" +
                            " <source file='" + cernvmCdrom  + "'/>\n" +
                            " <target dev='hdc' />\n<readonly />\n</disk>")
                            
        # Now the disk file or logical volume to provide the virtual hard drives

        if volumeGroup and measureVolumeGroup(volumeGroup):
          # Create logical volume for CernVM: fail if not able to do this

          try:
            self.createLogicalVolume()
          except Exception as e:
            raise VacError('Failed to create required logical volume: ' + str(e))
      
          root_disk_xml = ("<disk type='block' device='disk'>\n" +
                           " <driver name='qemu' type='raw' error_policy='report' cache='unsafe'/>\n" +
                           " <source dev='/dev/" + volumeGroup + "/" + self.name  + "'/>\n" +
                           " <target dev='" + machinetypes[self.machinetypeName]['root_device'] + 
                           "' bus='" + ("virtio" if "vd" in machinetypes[self.machinetypeName]['root_device'] else "ide") + "'/>\n</disk>")        

          # Unused
          rootDiskFileName = None

        else:
          # Create big empty disk file for CernVM

          try:
            gbDisk = (gbDiskPerProcessor if gbDiskPerProcessor else gbDiskPerProcessorDefault) * self.processors
          
            fTmp, rootDiskFileName = tempfile.mkstemp(prefix = 'root.disk.', dir = '/var/lib/vac/tmp')
            vac.vacutils.logLine('Make ' + str(gbDisk) + ' GB sparse file ' + rootDiskFileName)
            f = open(rootDiskFileName, 'ab')
            f.truncate(gbDisk * 1000000000)
            f.close()
          except:
            raise VacError('Creation of sparse disk image fails!')

          root_disk_xml = """<disk type='file' device='disk'><driver name='qemu' cache='unsafe' type='raw' error_policy='report' />
                             <source file='""" + rootDiskFileName + """' />
                             <target dev='""" + machinetypes[self.machinetypeName]['root_device'] + """' 
                              bus='""" + ("virtio" if "vd" in machinetypes[self.machinetypeName]['root_device'] else "ide") + """'/></disk>"""
      else:
        raise VacError('machine_model %s is not supported/recognised' % self.machineModel)
      
      try:
        conn = libvirt.open(None)
      except:
        raise VacError('exception when opening connection to the hypervisor')

      if conn == None:
        raise VacError('failed to open connection to the hypervisor')

      if os.path.isfile("/usr/libexec/qemu-kvm"):
        qemuKvmFile = "/usr/libexec/qemu-kvm"
      elif os.path.isfile("/usr/bin/qemu-kvm"):
        qemuKvmFile = "/usr/bin/qemu-kvm"
      else:
        return "qemu-kvm not in /usr/libexec or /usr/bin!"
      
      xmldesc=( """<domain type='kvm'>
  <name>""" + self.name + """</name>
  <uuid>""" + self.uuidStr + """</uuid>
  <memory unit='MiB'>""" + str(mbPerProcessor * self.processors) + """</memory>
  <currentMemory unit='MiB'>"""  + str(mbPerProcessor * self.processors) + """</currentMemory>
  <vcpu>""" + str(self.processors) + """</vcpu>
  <os>
    <type arch='x86_64' machine='pc'>hvm</type>
    <boot dev='cdrom'/>
    <bios useserial='yes'/>
  </os>
  <pm>
    <suspend-to-disk enabled='no'/>
    <suspend-to-mem  enabled='no'/>
  </pm>
  <cpu mode='host-passthrough'/>
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
    <emulator>""" + qemuKvmFile + """</emulator>""" + root_disk_xml + scratch_disk_xml + cernvm_cdrom_xml + """
    <interface type='network'>
      <mac address='""" + mac + """'/>
      <source network='vac_""" + natNetwork + """'/>
      <model type='virtio'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x03' function='0x0'/>
      <filterref filter='clean-traffic'/>
    </interface>
    <serial type="file">
      <source path="/var/lib/vac/machines/"""  + str(self.created) + '_' + self.machinetypeName + '_' + self.name +"""/console.log"/>
      <target port="1"/>
    </serial>                    
    <graphics type='vnc' port='"""  + str(5900 + self.ordinal) + """' keymap='en-gb'><listen type='address' address='127.0.0.1'/></graphics>
    <video>
      <model type='vga' vram='9216' heads='1'/>
      <address type='pci' domain='0x0000' bus='0x00' slot='0x02' function='0x0'/>
    </video>
  </devices>
</domain>
""" )

      try:
           dom = conn.createXML(xmldesc, 0)
      except Exception as e:
           vac.vacutils.logLine('Exception ("' + str(e) + '") when trying to create VM domain for ' + self.name)
           conn.close()
           raise VacError('exception when trying to create VM domain')
      finally:
           # If used, we unlink the big, sparse root disk image once libvirt has it open too,
           # so it disappears when libvirt is finished with it
           if rootDiskFileName:
             os.remove(rootDiskFileName)

      if not dom:
           vac.vacutils.logLine('Failed when trying to create VM domain for ' + self.name)
           conn.close()
           raise VacErrpr('failed when trying to create VM domain')
           
      conn.close()
       
      self.state = VacState.running
      
   def destroyVM(self, shutdownMessage = None):
      # Destory Virtual Machine running in this logical machine slot
   
      conn = libvirt.open(None)
      if conn == None:
          vac.vacutils.logLine('Failed to open connection to the hypervisor')
          raise VacError('failed to open connection to the hypervisor')

      try:
        dom = conn.lookupByName(self.name)
      except:
        vac.vacutils.logLine('VM %s has already gone' % self.name)
        conn.close()
        return
        
      try:
        dom.shutdown()
      except Exception as e:
        vac.vacutils.logLine('Failed to shutdown %s (%s) - already gone? paused?' % (self.name, str(e)))
      else:
        # 30s delay for any ACPI handler in the VM
        time.sleep(30.0)

      try:
        dom.destroy()
      except Exception as e:
        vac.vacutils.logLine('Failed to destroy %s (%s)' % (self.name, str(e)))

      conn.close()

   def createDC(self):
      # Create a Docker Container instance in this logical machine slot

      if machinetypes[self.machinetypeName]['root_image'].startswith('docker://'):
        image = machinetypes[self.machinetypeName]['root_image'][9:]
      else:
        raise VacError('Docker root_image must begin with "docker://"')

      os.makedirs(self.machinesDir() + '/mnt',
                  stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

      if volumeGroup and measureVolumeGroup(volumeGroup):
        # Create logical volume for Docker container

        try:
          self.createLogicalVolume()
        except Exception as e:
          raise VacError('Failed to create required logical volume: ' + str(e))

        try:
          os.system('/usr/sbin/mke2fs -t ext4 /dev/' + volumeGroup + '/' + self.name)
        except Exception as e:
          raise VacError('Failed to create filesystem: ' + str(e))
          
        try:
          os.system('/usr/bin/mount /dev/' + volumeGroup + '/' + self.name + ' ' + self.machinesDir() + '/mnt')
        except Exception as e:
          raise VacError('Failed to mount filesystem: ' + str(e))
          
      rwBindsList = [[self.machinesDir() + '/joboutputs', '/var/spool/joboutputs']]
      
      if 'tmp_binds' in machinetypes[self.machinetypeName]:
        for dir in machinetypes[self.machinetypeName]['tmp_binds']:
          tmp = tempfile.mkdtemp(prefix = dir.replace('/','_')[:30], dir = self.machinesDir() + '/mnt')
          os.chmod(tmp, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
          rwBindsList.append([tmp, dir])

      roBindsList = []
      
      roBindsList.append([self.machinesDir() + '/user_data', '/user_data'])
      os.chmod(self.machinesDir() + '/user_data', stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
       
      if machinetypes[self.machinetypeName]['cvmfs_repositories']:
        # Share everything mounted in cvmfs
        roBindsList.append(['/cvmfs','/cvmfs'])

        # Make sure the requested cvmfs repositories are mounted
        for repo in machinetypes[self.machinetypeName]['cvmfs_repositories']:
          try:
            os.listdir('/cvmfs/' + repo)
          except:
            pass

      roBindsList.extend([[self.machinesDir() + '/machinefeatures', '/etc/machinefeatures' ],
                          [self.machinesDir() + '/jobfeatures',     '/etc/jobfeatures'     ]])

      try:
        self.uuidStr = dockerRunCommand(rwBindsList, roBindsList, self.name, image,
                                        machinetypes[self.machinetypeName]['container_command'],
                                        self.processors * 1024, self.processors * mbPerProcessor * 1048576)
      except Exception as e:
        raise VacError('Failed to create Docker container %s (%s)' % (self.name, str(e)))
      else:
        vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/job_id',
                 self.uuidStr, stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
             
   def destroyDC(self, shutdownMessage = None):
     # Destroy the Docker Container instance running in this logical machine slot

     try:
       dockerRmCommand(self.name)
     except Exception as e:
       raise VacError('Failed to destroy Docker container %s (%s)!' % (self.name, str(e)))

   def createSC(self):
      # Create a Singularity Container instance in this logical machine slot
      
      if not singularityUser:
        raise VacError('Cannot create Singularity Containers if singularity_user is undefined!')
      
      if not os.path.isfile(singularityPath) or not os.access(singularityPath, os.X_OK):
        raise VacError('Cannot create Singularity Containers if %s executable does not exist!' % singularityPath)
      
      if machinetypes[self.machinetypeName]['root_image'].startswith('http://') or machinetypes[self.machinetypeName]['root_image'].startswith('https://'):
        try:
          image = vac.vacutils.getRemoteRootImage(machinetypes[self.machinetypeName]['root_image'], '/var/lib/vac/imagecache', '/var/lib/vac/tmp', 'Vac ' + vacVersion)
        except Exception as e:
          raise VacError(str(e))
      elif machinetypes[self.machinetypeName]['root_image'][0] == '/':
        # With SC, this might be an image file or a directory hierarchy (perhaps in /cvmfs/...)
        image = machinetypes[self.machinetypeName]['root_image']
      else:
        image = machinetypes[self.machinetypeName]['machinetype_path'] + '/files/' + machinetypes[self.machinetypeName]['root_image']

      os.makedirs(self.machinesDir() + '/mnt',
                  stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

      if volumeGroup and measureVolumeGroup(volumeGroup):
        # Create logical volume for Singularity

        try:
          self.createLogicalVolume()
        except Exception as e:
          raise VacError('Failed to create required logical volume: ' + str(e))

        try:
          os.system('/usr/sbin/mke2fs -t ext4 /dev/' + volumeGroup + '/' + self.name)
        except Exception as e:
          raise VacError('Failed to create filesystem: ' + str(e))
          
        try:
          os.system('/usr/bin/mount /dev/' + volumeGroup + '/' + self.name + ' ' + self.machinesDir() + '/mnt')
        except Exception as e:
          raise VacError('Failed to mount filesystem: ' + str(e))

      os.chown(self.machinesDir() + '/mnt', singularityUid, singularityGid)
      os.chown(self.machinesDir() + '/joboutputs', singularityUid, singularityGid)
      os.chmod(self.machinesDir() + '/user_data', stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)

      argsList = ['singularity', 
                  'exec',
                  '--contain',
                  '--workdir', self.machinesDir() + '/mnt']
 
      if machinetypes[self.machinetypeName]['cvmfs_repositories']:
        # Bind everything mounted in cvmfs
        argsList.extend(['--bind', '/cvmfs:/cvmfs'])
        # Make sure the requested cvmfs repositories are mounted
        for repo in machinetypes[self.machinetypeName]['cvmfs_repositories']:
          try:
            os.listdir('/cvmfs/' + repo)
          except:
            pass
             
      argsList.extend(['--bind', self.machinesDir() + '/machinefeatures:/tmp/machinefeatures',
                       '--bind', self.machinesDir() + '/jobfeatures:/tmp/jobfeatures',
                       '--bind', self.machinesDir() + '/joboutputs:/tmp/joboutputs',
                       '--bind', self.machinesDir() + '/user_data:/user_data',
                       image,
                       machinetypes[self.machinetypeName]['container_command']]) 

      vac.vacutils.logLine('Creating SC with ' + singularityPath + ' '.join(argsList[1:]))
     
      uuidSuffix = str(uuid.uuid4())
      pid = os.fork()
      
      if pid == 0:
        try:
          # Create new UTS namespace and set hostname within it
          try:
            libc = ctypes.CDLL("libc.so.6")
            libc.unshare(ctypes.c_int(0x04000000)) # = CLONE_NEWUTS
            libc.sethostname(ctypes.c_char_p(self.name), ctypes.c_int(len(self.name)))
            vac.vacutils.logLine('Set hostname to %s in UTS namespace ' % self.name)
          except Exception as e:
            vac.vacutils.logLine('Failed to set hostname to %s in UTS namespace (%s)' % (self.name, str(e)))

          # Set up CPU and memory cgroups
          pid = os.getpid()
          uuidStr = '%06d-%s' % (pid, uuidSuffix)
          
          os.makedirs(cpuCgroupFsRoot + '/vac/singularity-' + uuidStr, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
          with open(cpuCgroupFsRoot + '/vac/singularity-' + uuidStr + '/cgroup.procs', 'a') as f:
            f.write('%d\n' % pid)

          with open(cpuCgroupFsRoot + '/vac/singularity-' + uuidStr + '/cpu.shares', 'w') as f:
            f.write('%d\n' % (self.processors * 1024))

          os.makedirs(memoryCgroupFsRoot + '/vac/singularity-' + uuidStr, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
          with open(memoryCgroupFsRoot + '/vac/singularity-' + uuidStr + '/cgroup.procs', 'a') as f:
            f.write('%d\n' % pid)
          
          with open(memoryCgroupFsRoot + '/vac/singularity-' + uuidStr + '/memory.soft_limit_in_bytes', 'w') as f:
            f.write('%d\n' % (self.processors * mbPerProcessor * 1048576))

          # Start changing who we are
          os.chdir('/tmp')
          os.setgid(singularityGid)
          os.setuid(singularityUid)
          
          # Run singularity
          os.execv(singularityPath, argsList)

        except Exception as e:
          vac.vacutils.logLine('Forked subprocess for singularity command fails (%s)' % str(e))
          sys.exit(1)

      vac.vacutils.logLine('Singularity subprocess ' + str(pid) + ' for ' + self.name)
      createFile(self.machinesDir() + '/pid', str(pid))

      # Set job_id/UUID, always starting with PID for debugging
      self.uuidStr = '%06d-%s' % (pid, uuidSuffix)
      vac.vacutils.createFile(self.machinesDir() + '/jobfeatures/job_id',
                 self.uuidStr, stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
             
   def destroySC(self, shutdownMessage = None):
     # Destroy the Singularity Container instance in this logical machine slot

     try:
       scPid = int(open(self.machinesDir() + '/pid', 'r').read().strip())
     except:
       # Already gone???
       pass
     else:
       for pid in os.listdir('/proc'):
     
         if not pid.isdigit():
           continue
       
         try:
           uid = int(os.stat('/proc/' + pid).st_uid)
         except:
           # Failed to get process owner ID. Process gone?
           continue
         
         if uid != singularityUid:
           # Ignore processes unless they are owned by singularityUser
           continue
       
         try:
           pgid = int(open('/proc/' + pid, 'r').read().split(')')[1].split(' ')[3])
         except:
           # Failed to get process group ID. Process gone?
           continue

         if pgid == scPid:
           # Process in the process group of the SC head process, so kill it
           vac.vacutils.logLine('Kill Singularity Container process %s (%s)' % (pid, name))
           os.kill(int(pid), signal.SIG_KILL)

def measureVolumeGroup(vg):
      if not vg:
        return None
   
      try:
        return os.popen('LVM_SUPPRESS_FD_WARNINGS=1 /sbin/vgs --noheadings --options vg_size,extent_size --units b --nosuffix ' + vg, 'r').readline().strip().split()
      except Exception as e:
        vac.vacutils.logLine('Failed to measure size of volume group %s (%s)' % (vg, str(e)))
        return None

def dockerPsCommand():
      # Return a dictionary of currently defined Docker containers, filtered
      # by the pattern of names Vac creates on this host.
      # We use the docker command rather than the API for portability

      if not os.path.isfile(dockerPath) or not os.access(dockerPath, os.X_OK):
        return {}
      
      host,domain = os.uname()[1].split('.',1)

      # Get the output of docker ps
      pp = subprocess.Popen(dockerPath + ' ps --all --no-trunc --format "{{.Names}} {{.ID}} {{.Image}} {{.Status}} ."', 
                            shell=True, stdout=subprocess.PIPE).stdout

      containers = {}

      for line in pp:
        try:
          name, id, image, status, rest = line.split(None, 4)
        except:
          continue          

        if name.startswith(host + '-') and name.endswith('.' + domain):
          containers[name] = { "id" : id, "image" : image, "status" : status }
          
      pp.close()
      
      # Merge in values from the output of docker inspect
      if containers:
        pp = subprocess.Popen(dockerPath + ' inspect --format "{{.Name}} {{.State.Pid}}" ' + ' '.join([i for i in containers]),
                            shell=True, stdout=subprocess.PIPE).stdout

        for line in pp:
          try:
            name, pidStr = line.split()
          except:
            continue

          if name[0] == '/':
            name = name[1:]

          if name in containers:
            # Add pid value to the dictionary entry
            containers[name]['pid'] = int(pidStr)
          
        pp.close()
      
      return containers        

def dockerRunCommand(rwBindsList, roBindsList, name, image, script, cpuShares, memoryBytes):
      # Run a Docker container 
      # We use the docker command rather than the API for portability
      
      binds = ''
      
      for i in rwBindsList:
        binds += '-v %s:%s ' % (i[0], i[1])
            
      for i in roBindsList:
        binds += '-v %s:%s:ro ' % (i[0], i[1])
            
      cmd = dockerPath + ' run --detach --cpu-shares %d --memory-reservation %d %s --name %s --hostname %s %s %s' % (cpuShares, memoryBytes, binds, name, name, image, script)
      vac.vacutils.logLine('Creating DC with ' + cmd)

      pp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
                            
      id = pp.readline().strip()
      
      pp.close()
      return id

def dockerRmCommand(name):
      # Remove Docker container by name
      # We use the docker command rather than the API for portability
      
      subprocess.call(dockerPath + ' rm --force %s' % name, shell=True)

def checkNetwork():
      # Check and if necessary create network and set its attributes

      conn = libvirt.open(None)
      
      try:
           # Find the network if already defined
           vacNetwork = conn.networkLookupByName('vac_' + natNetwork)           
      except:
           vacNetwork = None
      else:
           if not re.search("<ip[^>]*address='" + factoryAddress + "'", vacNetwork.XMLDesc(1)):
             # The network does not have the right IP address!
             vac.vacutils.logLine('vac_' + natNetwork + ' defined with wrong IP address - removing!')

             try:
               vacNetwork.destroy()
             except:
               pass
            
             try:
               vacNetwork.undefine()
             except:
               pass

             # Probably need to do this too:                            
             fixNetworkingCommands()
             
             # Remember we removed it
             vacNetwork = None
           
      if not vacNetwork:
           # Doesn't exist so we define it
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

      # Make sure that the dummy module is loaded
      if os.system('/sbin/modprobe dummy') != 0:
        vac.vacutils.logLine('(Re)run of modprobe dummy fails!')
        return False

      # Make sure that the dummy0 interface exists
      # Should still return 0 even if dummy0 already exists, with any IP
      if os.system('/sbin/ifconfig dummy0 ' + dummyAddress) != 0:
        vac.vacutils.logLine('(Re)run of ifconfig dummy0 ' + dummyAddress + ' fails!')
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
        cmd = '/sbin/ifconfig dummy0 down'
        vac.vacutils.logLine('Trying  ' + cmd)
        os.system(cmd)
      except:
        pass
       
      try:
        cmd = '/bin/kill -9 `/bin/ps -C dnsmasq -o pid,args | /bin/egrep -- "--listen-address ' + factoryAddress + '|--conf-file=[^ ]*' + natNetwork + '.conf" | /bin/cut -f1 -d" "`'
        vac.vacutils.logLine('Trying  ' + cmd)
        os.system(cmd)
      except:
        pass

def cleanupOldMachines():
   # Remove files and directories associated with old machines

   machinesList = os.listdir('/var/lib/vac/machines')

   for machineDir in machinesList:
   
      try:   
        createdStr, machinetypeName, name = machineDir.split('_')
      except:
        continue

      if machinetypeName not in machinetypes: 
        # use 3 days for machinetypes that have been removed
        machines_dir_days = 3.0
      else: 
        # use the per-machinetype value
        machines_dir_days = machinetypes[machinetypeName]['machines_dir_days']

      if machines_dir_days <= 0.0:
        # if zero then we do not expire these directories at all
        continue

      try:
        if (os.stat('/var/lib/vac/machines/' + machineDir + '/heartbeat').st_mtime < int(time.time() - machines_dir_days * 86400)):
          vac.vacutils.logLine('Deleting expired ' + machineDir)
          shutil.rmtree('/var/lib/vac/machines/' + machineDir)
      except:
        # Skip if no heartbeat yet
        continue

def makeMjfBody(created, machinetypeName, machineName, path):

   if '/../' in path:
     # Obviously not ok
     return None

   # Fold // to /
   requestURI = path.replace('//','/')

   splitRequestURI = requestURI.split('/')

   if len(splitRequestURI) > 3:
     # Subdirectories are now allowed
     return None

   machinesDir = '/var/lib/vac/machines/' + str(created) + '_' + machinetypeName + '_' + machineName

   if requestURI == '/machinefeatures/' or \
      requestURI == '/machinefeatures' or \
      requestURI == '/jobfeatures/' or \
      requestURI == '/jobfeatures':
     # Make HTML directory listing
     try:
       body = '<html><body><ul>'

       for fileName in os.listdir(machinesDir + '/' + splitRequestURI[1]):
         body += '<li><a href="' + fileName + '">' + fileName + '</a></li>'
         
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

def makeMetadataBody(created, machinetypeName, machineName, path):

   machinesDir = '/var/lib/vac/machines/' + str(created) + '_' + machinetypeName + '_' + machineName

   # Fold // to /, and /latest/ to something that will match a dated version
   requestURI = path.replace('//','/').replace('/latest/','/0000-00-00/')

   # EC2 or OpenStack user-data
   if re.search('^/[0-9]{4}-[0-9]{2}-[0-9]{2}/user-data$|^/openstack/[0-9]{4}-[0-9]{2}-[0-9]{2}/user-data$', requestURI):
     try:
       return open(machinesDir + '/user_data', 'r').read()
     except Exception as e:
       return None

   # EC2 or OpenStack meta-data.json
   if re.search('^/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data\.json$|^/openstack/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data\.json$', requestURI):
     metaData = { 'availability_zone': os.uname()[1] }

     try:
       publicKey = open(machinesDir + '/root_public_key', 'r').read()
     except:
       pass
     else:
       metaData['public_keys'] = { "0" : publicKey }

     try:
      uuidStr = open(machinesDir + '/jobfeatures/job_id', 'r').read()
     except:
      pass
     else:
      metaData['uuid'] = uuidStr

     metaData['hostname'] = machineName
     metaData['name']     = machineName

     metaData['meta'] = {
                           'machinefeatures' : 'http://' + mjfAddress + '/machinefeatures',
                           'jobfeatures'     : 'http://' + mjfAddress + '/jobfeatures',
                           'joboutputs'      : 'http://' + mjfAddress + '/joboutputs',
                           'machinetype'     : machinetypeName
                         }      
     try:
       return json.dumps(metaData)
     except Exception as e:
       return None

   # meta-data directory listing
   if re.search('^/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data/?$|^/openstack/[0-9]{4}-[0-9]{2}-[0-9]{2}/meta-data/?$', requestURI):
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
       return open(machinesDir + '/jobfeatures/job_id', 'r').read()
     except:
       return None

   # No body (and therefore 404) if we don't recognise the request
   return None

def writePutBody(created, machinetypeName, machineName, path, body):

   # Fold // to /
   requestURI = path.replace('//','/')
   
   splitRequestURI = requestURI.split('/')
   
   if len(splitRequestURI) != 3 or splitRequestURI[1] != 'joboutputs' or not splitRequestURI[2]:
     return False

   machinesDir = '/var/lib/vac/machines/' + str(created) + '_' + machinetypeName + '_' + machineName
   
   try:
     vac.vacutils.createFile(machinesDir + '/joboutputs/' + splitRequestURI[2],
                             body,
                             stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
   except Exception as e:
     vac.vacutils.logLine('Failed to write ' + machinesDir + '/joboutputs/' + splitRequestURI[2] + ' (' + str(e) + ')')
     return False

   vac.vacutils.logLine('Wrote ' + machinesDir + '/joboutputs/' + splitRequestURI[2])
   return True

def sendMachinetypesRequests(factoryList = None, clientName = '-'):

   salt = base64.b64encode(os.urandom(32))
   sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   sock.settimeout(udpTimeoutSeconds / vacqueryTries)
   setSockBufferSize(sock)

   # Initialise dictionary of per-factory, per-machinetype responses
   responses = {}

   if factoryList is None:
     factoryList = factories

   for rawFactoryName in factoryList:
     responses[canonicalFQDN(rawFactoryName)] = { 'machinetypes' : {} }

   queryCount = 0
   
   # We just use integer second counting for now, despite the config file
   while queryCount <= vacqueryTries:
     queryCount += 1

     requestsSent = 0
     for rawFactoryName in factoryList:
     
       factoryName = canonicalFQDN(rawFactoryName)

       try:
         numMachinetypes = responses[factoryName]['num_machinetypes']
       except:
         # We initially expect every factory to tell us about at least 1 machinetype
         numMachinetypes = 1
     
       # Send out requests to all factories with insufficient replies so far
       if len(responses[factoryName]['machinetypes']) < numMachinetypes:

         requestsSent += 1
         try:          
           sock.sendto(json.dumps({'vac_version'      : 'Vac ' + vacVersion + ' ' + clientName,
                                   'vacquery_version' : 'VacQuery ' + vac.shared.vacQueryVersion,
                                   'space'            : spaceName,
                                   'cookie'           : hashlib.sha256(salt + factoryName).hexdigest(),
                                   'message_type'     : 'machinetypes_query'}),
                       (factoryName,995))

         except socket.error:
           pass

     if requestsSent == 0:
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
           if 'message_type' in response and response['message_type'] == 'machinetype_status' and \
              'cookie' 			in response and \
              'space' 			in response and \
              response['space']  == spaceName and \
              'factory' 		in response and \
              response['cookie'] == hashlib.sha256(salt + response['factory']).hexdigest() and \
              'num_machinetypes'	in response and \
              'machinetype'		in response and \
              'running_hs06'		in response and \
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

def sendMachinesRequests(factoryList = None, clientName = '-'):

   salt = base64.b64encode(os.urandom(32))
   sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   sock.settimeout(udpTimeoutSeconds / vacqueryTries)
   setSockBufferSize(sock)

   # Initialise dictionary of per-factory, per-machine responses
   responses = {}

   if factoryList is None:
     factoryList = factories

   for rawFactoryName in factoryList:   
     responses[vac.shared.canonicalFQDN(rawFactoryName)] = { 'machines' : {} }

   queryCount = 0
   
   # We just use integer second counting for now, despite the config file
   while queryCount <= vacqueryTries:
     queryCount += 1

     requestsSent = 0
     for rawFactoryName in factoryList:

       factoryName = canonicalFQDN(rawFactoryName)

       try:
         numMachines = responses[factoryName]['num_machines']
       except:
         # We initially expect every factory to tell us about at least 1 machine
         numMachines = 1
     
       # Send out requests to all factories with insufficient replies so far
       if len(responses[factoryName]['machines']) < numMachines:

         requestsSent += 1
         try:
           sock.sendto(json.dumps({'vac_version'      : 'Vac ' + vacVersion + ' ' + clientName,
                                   'vacquery_version' : 'VacQuery ' + vac.shared.vacQueryVersion,
                                   'space'            : spaceName,
                                   'cookie'           : hashlib.sha256(salt + factoryName).hexdigest(),
                                   'method'           : 'machines', # will be deprecated
                                   'message_type'     : 'machines_query'}),
                       (factoryName,995))

         except socket.error:
           pass

     if requestsSent == 0:
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
           if 'message_type' in response and response['message_type'] == 'machine_status' and \
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

def sendFactoriesRequests(factoryList = None, clientName = '-'):

   salt = base64.b64encode(os.urandom(32))
   sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
   sock.settimeout(udpTimeoutSeconds / vacqueryTries)
   setSockBufferSize(sock)

   # Initialise dictionary of per-factory responses
   responses = {}

   if factoryList is None:
     factoryList = factories

   queryCount = 0
   
   # We just use integer second counting for now, despite the config file
   while queryCount <= vacqueryTries:
     queryCount += 1

     requestsSent = 0
     for rawFactoryName in factoryList:

       factoryName = canonicalFQDN(rawFactoryName)
     
       # Send out requests to all factories with insufficient replies so far
       if factoryName not in responses:

         requestsSent += 1
         try:          
           sock.sendto(json.dumps({'vac_version'      : 'Vac ' + vacVersion,
                                   'vacquery_version' : 'VacQuery ' + vac.shared.vacQueryVersion,
                                   'space'            : spaceName,
                                   'cookie'           : hashlib.sha256(salt + factoryName).hexdigest(),
                                   'method'           : 'factories',
                                   'message_type'     : 'factory_query'}),
                       (factoryName,995))

         except socket.error:
           pass

     if requestsSent == 0:
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

           if 'message_type' in response and response['message_type'] == 'factory_status' and \
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

def makeMachineResponse(cookie, ordinal, clientName = '-', timeNow = None):

   if not timeNow:
     timeNow = int(time.time())

   lm = VacSlot(ordinal, forResponder = True)

   if lm.hs06:
     hs06 = lm.hs06
   else:
     hs06 = 1.0 * lm.processors

   responseDict = {
                'message_type'		: 'machine_status',
                'vac_version'		: 'Vac ' + vacVersion + ' ' + clientName, # renamed in Vacuum Platform 2.0 spec
                'daemon_version'	: 'Vac ' + vacVersion + ' ' + clientName,
                'vacquery_version'	: 'VacQuery ' + vacQueryVersion,
                'cookie'	  	: cookie,
                'space'		    	: spaceName,
                'factory'       	: os.uname()[1],
                'num_machines'       	: numMachineSlots,
                'time_sent'		: timeNow,

                'machine' 		: lm.name,
                'state'			: lm.state,
                'machine_model'		: lm.machineModel,
                'uuid'			: lm.uuidStr,
                'created_time'		: lm.created,
                'started_time'		: lm.started,
                'heartbeat_time'	: lm.heartbeat,
                'num_cpus'		: lm.processors, # removed in Vacuum Platform 2.0 spec
                'num_processors'	: lm.processors,
                'cpu_seconds'		: lm.cpuSeconds,
                'cpu_percentage'	: lm.cpuPercentage,
                'hs06' 		       	: hs06,
                'machinetype'		: lm.machinetypeName,
                'shutdown_message'  	: lm.shutdownMessage,
                'shutdown_time'     	: lm.shutdownMessageTime
                  }

   if gocdbSitename:
     responseDict['site'] = gocdbSitename
   else:
     responseDict['site'] = '.'.join(spaceName.split('.')[1:]) if '.' in spaceName else spaceName

   if lm.accountingFqan:
     responseDict['fqan'] = lm.accountingFqan

   return json.dumps(responseDict)

def makeMachinetypeResponses(cookie, clientName = '-'):
   # Send back machinetype messages to the querying factory or client
   responses = []
   timeNow = int(time.time())

   # Go through the machinetypes
   for machinetypeName in machinetypes:

     runningHS06       = 0.0
     numBeforeFizzle   = 0
     runningMachines   = 0
     runningProcessors = 0

     # Go through the VM slots, looking for starting/running instances of this machinetype
     for ordinal in range(numMachineSlots):

       name = nameFromOrdinal(ordinal)

       try:
         (createdStr, machinetypeNameTmp, machineModel) = open('/var/lib/vac/slots/' + name,'r').read().split()
         created = int(createdStr)
       except:
         continue

       if machinetypeNameTmp != machinetypeName:
         continue

       machinesDir = '/var/lib/vac/machines/' + str(created) + '_' + machinetypeName + '_' + name
       if not os.path.isdir(machinesDir):
         # machines directory has been cleaned up?
         continue

       try:
         timeStarted = int(os.stat(machinesDir + '/started').st_ctime)
       except:
         timeStarted = None

       try:
         timeHeartbeat = int(os.stat(machinesDir + '/heartbeat').st_ctime)
       except:
         timeHeartbeat = None

       try:                  
         numProcessors = float(open(machinesDir + '/jobfeatures/allocated_cpu', 'r').readline())
       except:
         numProcessors = 1

       try:                  
         hs06 = float(open(machinesDir + '/jobfeatures/hs06_job', 'r').readline())
       except:
         hs06 = 1.0 * numProcessors

       hasFinished = os.path.exists(machinesDir + '/finished')

       # some hardcoded timeouts here in case old files are left lying around 
       # this means that old files are ignored when working out the state
       if (timeStarted and 
           timeHeartbeat and 
           (timeHeartbeat > int(time.time() - 3600)) and
           not hasFinished):
         # Running
         runningHS06       += hs06
         runningMachines   += 1
         runningProcessors += numProcessors

         if int(time.time()) < timeStarted + machinetypes[machinetypeName]['fizzle_seconds']:
           numBeforeFizzle += 1

       elif not timeStarted and (created > int(time.time() - 3600)):
         # Starting
         runningHS06       += hs06
         runningMachines   += 1
         runningProcessors += 1
         numBeforeFizzle   += 1         

     # Outcome of the most recently created instance of this machinetype that has already finished

     shutdownMessage     = None
     shutdownMessageTime = None
     shutdownMachineName = None

     try:
       # Updated by createFinishedFile()
       shutdownCreated, shutdownMachinetypeName, shutdownMachineName = open('/var/lib/vac/finishes/' + machinetypeName, 'r').readline().strip().split()
       
     except:
       pass
     else:
       try:
         shutdownMessage = open('/var/lib/vac/machines/%s_%s_%s/joboutputs/shutdown_message' % (shutdownCreated, shutdownMachinetypeName, shutdownMachineName),'r').readline().strip()
         messageCode = int(shutdownMessage[0:3])
         shutdownMessageTime = int(os.stat('/var/lib/vac/machines/%s_%s_%s/joboutputs/shutdown_message' % (shutdownCreated, shutdownMachinetypeName, shutdownMachineName)).st_ctime)
       except:
         # No explicit shutdown message with a message code, so we make one up if necessary
         
         try:
           timeStarted   = int(os.stat(dir + '/started').st_ctime)
           timeHeartbeat = int(os.stat(dir + '/heartbeat').st_ctime)
         except:
           pass
         else:
           if (timeHeartbeat - timeStarted) < machinetypes[machinetypeName]['fizzle_seconds']:
             shutdownMessageTime = timeHeartbeat
             shutdownMessage = '300 Vac detects fizzle after ' + str(timeHeartbeat - timeStarted) + ' seconds'

     responseDict = {
                'message_type'		: 'machinetype_status',
                'vac_version'		: 'Vac ' + vacVersion + ' ' + clientName, # removed in Vacuum Platform 2.0 spec
                'daemon_version'	: 'Vac ' + vacVersion + ' ' + clientName,
                'vacquery_version'	: 'VacQuery ' + vacQueryVersion,
                'cookie'	  	: cookie,
                'space'		    	: spaceName,
                'factory'       	: os.uname()[1],
                'num_machinetypes'      : len(machinetypes),
                'time_sent'		: timeNow,

                'machinetype'		: machinetypeName,
                'running_hs06'        	: runningHS06,
                'running_machines'      : runningMachines,
                'running_cpus'          : runningProcessors, # removed in Vacuum Platform 2.0 spec
                'running_processors'    : runningProcessors,
                'num_before_fizzle' 	: numBeforeFizzle,
                'shutdown_message'  	: shutdownMessage,
                'shutdown_time'     	: shutdownMessageTime,
                'shutdown_machine'  	: shutdownMachineName
                     }

     if gocdbSitename:
       responseDict['site'] = gocdbSitename
     else:
       responseDict['site'] = '.'.join(spaceName.split('.')[1:]) if '.' in spaceName else spaceName
       
     try:
       responseDict['fqan'] = machinetypes[machinetypeName]['accounting_fqan']
     except:
       pass

     responses.append(json.dumps(responseDict))

   return responses
   
def makeFactoryResponse(cookie, clientName = '-'):
   # Send back factory status message to the querying client

   vacDiskStatFS  = os.statvfs('/var/lib/vac')
   rootDiskStatFS = os.statvfs('/tmp')
   
   memory = vac.vacutils.memInfo()

   try:
     counts = open('/var/lib/vac/counts','r').readline().split()
     runningMachines   = int(counts[0])
     runningProcessors = int(counts[2])
     runningHS06       = float(counts[4])
   except:
     runningProcessors = 0
     runningMachines   = 0
     runningHS06       = 0

   try:
     factoryHeartbeatTime = int(os.stat('/var/lib/vac/factory-heartbeat').st_ctime)
   except:
     factoryHeartbeatTime = 0

   try:
     responderHeartbeatTime = int(os.stat('/var/lib/vac/responder-heartbeat').st_ctime)
   except:
     responderHeartbeatTime = 0

   try:
     mjfHeartbeatTime = int(os.stat('/var/lib/vac/mjf-heartbeat').st_ctime)
   except:
     mjfHeartbeatTime = 0

   try:
     metadataHeartbeatTime = int(os.stat('/var/lib/vac/metadata-heartbeat').st_ctime)
   except:
     metadataHeartbeatTime = 0

   try:
     osIssue = open('/etc/redhat-release.vac','r').readline().strip()
   except:
     try:
       osIssue = open('/etc/redhat-release','r').readline().strip()
     except:
       osIssue = os.uname()[2]

   try:
     bootTime = int(time.time() - float(open('/proc/uptime','r').readline().split()[0]))
   except:
     bootTime = 0
     
   if hs06PerProcessor:
     maxHS06 = numProcessors * hs06PerProcessor
   else:
     maxHS06 = numProcessors * 1.0

   responseDict = {
                'message_type'		   : 'factory_status',
                'vac_version'		   : 'Vac ' + vacVersion + ' ' + clientName, # renamed in Vacuum Platform 2.0 spec
                'daemon_version'	   : 'Vac ' + vacVersion + ' ' + clientName,
                'vacquery_version'	   : 'VacQuery ' + vacQueryVersion,
                'cookie'	  	   : cookie,
                'space'		    	   : spaceName,
                'factory'       	   : os.uname()[1],
                'time_sent'		   : int(time.time()),

                'running_cpus'             : runningProcessors, # renamed in Vacuum Platform 2.0 spec
                'running_processors'       : runningProcessors,
                'running_machines'         : runningMachines,
                'running_hs06'             : runningHS06,
                'max_cpus'		   : numProcessors,	# renamed in Vacuum Platform 2.0 spec
                'max_processors'	   : numProcessors,
                'max_machines'             : numProcessors,
                'max_hs06'		   : maxHS06,

                'total_cpus'		   : numProcessors,	# deprecated
                'total_machines'           : numProcessors,	# deprecated
                'total_hs06'		   : maxHS06,		# deprecated

                'root_disk_avail_kb'       : (rootDiskStatFS.f_bavail * rootDiskStatFS.f_frsize) / 1024,
                'root_disk_avail_inodes'   : rootDiskStatFS.f_favail,

                'vac_disk_avail_kb'        : ( vacDiskStatFS.f_bavail *  vacDiskStatFS.f_frsize) / 1024, # renamed in Vacuum Platform 2.0 spec
                'daemon_disk_avail_kb'      : ( vacDiskStatFS.f_bavail *  vacDiskStatFS.f_frsize) / 1024,
                'vac_disk_avail_inodes'    :  vacDiskStatFS.f_favail,                                    # renamed in Vacuum Platform 2.0 spec
                'daemon_disk_avail_inodes'  :  vacDiskStatFS.f_favail,

                'load_average'		   : vac.vacutils.loadAvg(2),
                'kernel_version'	   : os.uname()[2],
                'os_issue'		   : osIssue,
                'boot_time'		   : bootTime,
                'factory_heartbeat_time'   : factoryHeartbeatTime,
                'responder_heartbeat_time' : responderHeartbeatTime,
                'mjf_heartbeat_time'       : mjfHeartbeatTime,
                'metadata_heartbeat_time'  : metadataHeartbeatTime,
                'swap_used_kb'		   : memory['SwapTotal'] - memory['SwapFree'],
                'swap_free_kb'		   : memory['SwapFree'],
                'mem_used_kb'		   : memory['MemTotal'] - memory['MemFree'],
                'mem_total_kb'		   : memory['MemTotal']
                  }

   if gocdbSitename:
     responseDict['site'] = gocdbSitename
   else:
     responseDict['site'] = '.'.join(spaceName.split('.')[1:]) if '.' in spaceName else spaceName

   return json.dumps(responseDict)

def updateSpaceCensus():
   # Update the files in /var/lib/vac/space-census, one per working factory in this space,
   # based on VacQuery responses. Returns the number of factory responses in that 
   # directory dated within the last gocdbUpdateSeconds.
   
   try:
     os.makedirs('/var/lib/vac/space-census', 
                 stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
   except:
     pass

   try:
     responses = vac.shared.sendFactoriesRequests()
   except Exception as e:
     vac.vacutils.logLine('Failed to gather factory responses for space census ("' + str(e) + '")')
   else:
     for factoryName in responses:   
       try:
         vac.vacutils.createFile('/var/lib/vac/space-census/' + factoryName, json.dumps(responses[factoryName]), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, '/var/lib/vac/tmp')
       except Exception as e:
         vac.vacutils.logLine('Failed write census response from ' + factoryName + ' ("' + str(e) + '")')
   
   # Go through the factory files, counting recent ones and deleting old ones
   censusCount = 0
   now = int(time.time())
   
   for factoryName in os.listdir('/var/lib/vac/space-census'):
     try:
       factoryTime = int(os.stat('/var/lib/vac/space-census/' + factoryName).st_ctime)
     except:
       continue
     else:
       if now < factoryTime + gocdbUpdateSeconds:
         censusCount += 1
       elif now > factoryTime + 2 * gocdbUpdateSeconds:
         # Use twice the update frequency for debugging at the command line
         vac.vacutils.logLine('Removed expired space census file for factory ' + factoryName)
         os.remove('/var/lib/vac/space-census/' + factoryName)
         
   return censusCount

def updateGOCDB():

   vac.vacutils.logLine('Updating GOCDB')

   # Initialisations
   factoriesCount = 0
   maxProcessors = 0
   maxMachines = 0
   maxHS06 = 0
   now = int(time.time())
   spaceCatalogue = {}

   # First load a static catalogue of factories if it exists 
   #
   # A JSON dictionary of dictionaries, with this format: 
   #
   #   {'vac01.example.com': {'max_processors':2, 'max_machines':1, 'max_hs06':20.0},
   #    'vac02.example.com': {'max_processors':3, 'max_machines':2, 'max_hs06':30.0} }
   # 
   if os.path.exists('/var/lib/vac/space-catalogue.json'):
     try:
       spaceCatalogue = json.loads(open('/var/lib/vac/space-catalogue.json', 'r').read())
     except Exception as e:
       vac.vacutils.logLine('Failed to parse space-catalogue.json (' + str(e) + ')')
     else:
       for factoryName in spaceCatalogue:
         try:
           maxProcessors += spaceCatalogue[factoryName]['max_processors']
           maxMachines += spaceCatalogue[factoryName]['max_machines']
           maxHS06 += spaceCatalogue[factoryName]['max_hs06']
           factoriesCount += 1
         except:
           vac.vacutils.logLine('Failed to parse space-catalogue.json item ' + str(spaceCatalogue[factoryName]))

   # Then go through the dynamic responses, for factories not already counted
   for factoryName in os.listdir('/var/lib/vac/space-census'):
     if factoryName in spaceCatalogue:
       # Don't double count factories also in the static catalogue file
       continue
   
     try:
       factoryTime = int(os.stat('/var/lib/vac/space-census/' + factoryName).st_ctime)
     except:
       continue
     else:
       if now < factoryTime + gocdbUpdateSeconds:
         try:
           factoryResponse = json.loads(open('/var/lib/vac/space-census/' + factoryName, 'r').read())
           maxProcessors += factoryResponse['max_processors']
           maxMachines += factoryResponse['max_machines']
           maxHS06 += factoryResponse['max_hs06']
           factoriesCount += 1
         except Exception as e:
           vac.vacutils.logLine('Failed to parse census response from ' + factoryName + ' ("' + str(e) + '")')

   voShares = {}
   policyRules = ''
   sharesTotal = 0.0

   for machinetypeName in machinetypes:
     if 'accounting_fqan' in machinetypes[machinetypeName]:
       policyRules += 'VOMS:' + machinetypes[machinetypeName]['accounting_fqan'] + ','

       try:
         targetShare = float(machinetypes[machinetypeName]['share'])
       except:
         targetShare = 0.0
         
       if targetShare:
         try:
           voName = machinetypes[machinetypeName]['accounting_fqan'].split('/')[1]
         except:
           pass
         else:
           if voName in voShares:
             voShares[voName] += targetShare
           else:
             voShares[voName] = targetShare
           
           sharesTotal += targetShare
           
   otherInfo = ''
   
   for voName in voShares:
     otherInfo += 'Share=%s:%d,' % (voName, int(0.5 + (100 * voShares[voName]) / sharesTotal))

   spaceValues = {
       'ComputingManagerCreationTime':		datetime.datetime.utcnow().replace(microsecond = 0).isoformat() + 'Z',
       'ComputingManagerProductName':		'Vac',
       'ComputingManagerProductVersion':	vacVersion,
       'ComputingManagerTotalLogicalCPUs':	maxProcessors,
       'ComputingManagerTotalSlots':		maxMachines,
       'ComputingManagerTotalPhysicalCPUs':	factoriesCount, # factories, not really physical CPUs (x2 ?)
       'BenchmarkType':				'specint2000',
       'BenchmarkValue':			maxHS06 * 250.0
     }
     
   if otherInfo:
     spaceValues['ComputingManagerOtherInfo'] = otherInfo.strip(',')
     
   if policyRules:
     spaceValues['PolicyRule'] = policyRules.strip(',')
     spaceValues['PolicyScheme'] = 'org.glite.standard'

   vac.vacutils.logLine('Space info for Vac service %s in GOCDB site %s: %s' 
                          % (spaceName, gocdbSitename, str(spaceValues)))

   try:
     vac.vacutils.updateSpaceInGOCDB(
       gocdbSitename,
       spaceName,
       'uk.ac.gridpp.vac',
       gocdbCertFile,
       gocdbKeyFile,
       '/etc/grid-security/certificates',
       'Vac ' + vacVersion,
       spaceValues,
       None # ONCE GOCDB ALLOWS API CREATION OF ENDPOINTS WE CAN PUT MORE INFO (eg wallclock limits) THERE
            # ONE ENDPOINT OF THE VAC SERVICE PER MACHINETYPE
       )
   except Exception as e:
     vac.vacutils.logLine('Failed to update space info in GOCDB: ' + str(e))
   else:
     vac.vacutils.logLine('Successfully updated space info in GOCDB')
    
def createFile(targetname, contents, mode=stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP):
   # Create a temporary text file containing contents then move
   # it into place. Rename is an atomic operation in POSIX,
   # including situations where targetname already exists.

   try:
     ftup = tempfile.mkstemp(prefix = 'temp', dir = '/var/lib/vac/tmp', text = True)
     os.write(ftup[0], contents)
       
     if mode:
       os.fchmod(ftup[0], mode)

     os.close(ftup[0])
     os.rename(ftup[1], targetname)
     return True
     
   except Exception as e:
     vac.vacutils.logLine('createFile(' + targetname + ',...) fails with "' + str(e) + '"')
     
     try:
       os.remove(ftup[1])
     except:
       pass

     return False
     