#
#  vacutils.py - reusable functions and classes for Vac and Vcycle
#
## THE DEFINITIVE SOURCE OF THIS FILE IS THE Vac GIT REPOSITORY ##
## UNMODIFIED VERSIONS ARE COPIED TO THE Vcycle REPO AS NEEDED  ##
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-9. All rights reserved.
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
import re
import sys
import stat
import time
import glob
import json
import ctypes
import string
import urllib
import StringIO
import tempfile
import calendar
import hashlib
import xml.etree.cElementTree
import pycurl
import base64
import M2Crypto

logStream = sys.stdout

class VacutilsError(Exception):
   pass

def logToStderr():
   global logStream
   logStream = sys.stderr

def logLine(text):
   logStream.write(time.strftime('%b %d %H:%M:%S [') + str(os.getpid()) + ']: ' + text + '\n')
   logStream.flush()

def createFile(targetname, contents, mode=stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP, tmpDir = None):
   # Create a temporary text file containing contents then move
   # it into place. Rename is an atomic operation in POSIX,
   # including situations where targetname already exists.

   if tmpDir is None:
     tmpDir = os.path.dirname(targetname)

   try:
     ftup = tempfile.mkstemp(prefix = 'createFile-', dir = tmpDir, text = True)
     os.write(ftup[0], contents)

     if mode:
       os.fchmod(ftup[0], mode)

     os.close(ftup[0])
     os.rename(ftup[1], targetname)
     return True
   except Exception as e:
     logLine('createFile(' + targetname + ',...) fails with "' + str(e) + '"')

     try:
       os.remove(ftup[1])
     except:
       pass

     return False

def secondsToHHMMSS(seconds):
   hh, ss = divmod(seconds, 3600)
   mm, ss = divmod(ss, 60)
   return '%02d:%02d:%02d' % (hh, mm, ss)

def secondsToString(timeStamp):

   if timeStamp is None or timeStamp == 0:
     return ' - '

   seconds = int(time.time() - timeStamp)

   if seconds < 120:
     return str(seconds) + 's'
   elif seconds < 7200:
     return '%dm' % (seconds / 60)
   elif seconds < 172800:
     return '%dh' % (seconds / 3600)
   else:
     return '%dd' % (seconds / 86400)

def readPipe(pipesCache, pipeURL, versionString, updatePipes = False):

   # Default value in case not given in file
   cacheSeconds = 3600

   pipeFile = pipesCache + '/' + urllib.quote(pipeURL, '')

   try:
     pipeDict = json.load(open(pipeFile, 'r'))
   except:
     # No pipe file found, so create a placeholder file
     logLine('Unable to read and parse vacuum pipe file ' + pipeFile)
     pipeDict = { 'cache_seconds' : cacheSeconds, 'machinetypes' : [] }

     if not updatePipes:
       # if not updating then nothing further we can do
       return pipeDict
     
     try:
       f = open(pipeFile, 'w')
     except:
       raise VacutilsError('Unable to write vacuum pipe file ' + pipeFile)
     else:
       json.dump(pipeDict, f)
       f.close()

     # but still force an attempt to fetch remote file
     cacheSeconds = 0

   else:
     try:
       cacheSeconds = int(pipeDict['cache_seconds'])
     except:
       pipeDict['cache_seconds'] = cacheSeconds

   # Check if cache seconds has expired
   if updatePipes and \
      (cacheSeconds == 0 or 
       int(os.stat(pipeFile).st_mtime) <= time.time() - cacheSeconds) and \
      ((pipeURL[0:7] == 'http://') or (pipeURL[0:8] == 'https://')):
     buffer = StringIO.StringIO()
     c = pycurl.Curl()
     c.setopt(c.URL, pipeURL)
     c.setopt(c.WRITEFUNCTION, buffer.write)
     c.setopt(c.USERAGENT, versionString)
     c.setopt(c.TIMEOUT, 30)
     c.setopt(c.FOLLOWLOCATION, True)
     c.setopt(c.SSL_VERIFYPEER, 1)
     c.setopt(c.SSL_VERIFYHOST, 2)

     if os.path.isdir('/etc/grid-security/certificates'):
       c.setopt(c.CAPATH, '/etc/grid-security/certificates')
     else:
       logLine('/etc/grid-security/certificates directory does not exist - relying on curl bundle of commercial CAs')

     logLine('Fetching ' + pipeURL)

     try:
       c.perform()
     except Exception as e:
       raise VacutilsError('Failed to read ' + pipeURL + ' (' + str(e) + ')')

     c.close()

     try:
       pipeDict = json.loads(buffer.getvalue())
     except:
       raise VacutilsError('Failed to load vacuum pipe file from ' + pipeURL)
     else:
       try:
         f = open(pipeFile, 'w')
       except:
         logLine('Unable to write vacuum pipe file ' + pipeFile)
         return pipeDict
       else:
         json.dump(pipeDict, f)
         f.close()
         logLine('Saved ' + pipeURL + ' as ' + pipeFile)

     createFile(pipeFile, json.dumps(pipeDict), stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

   return pipeDict

def createUserData(shutdownTime, machinetypePath, options, versionString, spaceName, machinetypeName, userDataPath, hostName, uuidStr,
                   machinefeaturesURL = None, jobfeaturesURL = None, joboutputsURL = None, rootImageURL = None, heartbeatMachinesURL = None,
                   gocdbSitename = None):

   # Get raw user_data template file, either from network ...
   if (userDataPath[0:7] == 'http://') or (userDataPath[0:8] == 'https://'):
     buffer = StringIO.StringIO()
     c = pycurl.Curl()
     c.setopt(c.URL, userDataPath)
     c.setopt(c.WRITEFUNCTION, buffer.write)
     c.setopt(c.USERAGENT, versionString)
     c.setopt(c.TIMEOUT, 30)
     c.setopt(c.FOLLOWLOCATION, True)
     c.setopt(c.SSL_VERIFYPEER, 1)
     c.setopt(c.SSL_VERIFYHOST, 2)

     if os.path.isdir('/etc/grid-security/certificates'):
       c.setopt(c.CAPATH, '/etc/grid-security/certificates')
     else:
       logLine('/etc/grid-security/certificates directory does not exist - relying on curl bundle of commercial CAs')

     try:
       c.perform()
     except Exception as e:
       raise VacutilsError('Failed to read ' + userDataPath + ' (' + str(e) + ')')

     c.close()

     # We only do this substitution if it was an HTTP(S) URL
     userDataContents = buffer.getvalue().replace('##user_data_url##', userDataPath)

   # ... or from filesystem
   else:
     if userDataPath[0] == '/':
       userDataFile = userDataPath
     else:
       userDataFile = machinetypePath + '/files/' + userDataPath

     try:
       u = open(userDataFile, 'r')
       userDataContents = u.read()
       u.close()
     except:
       raise VacutilsError('Failed to read ' + userDataFile)

   # Default substitutions (plus ##user_data_url## possibly done already)
   if gocdbSitename:
     userDataContents = userDataContents.replace('##user_data_site##', gocdbSitename)
   
   userDataContents = userDataContents.replace('##user_data_space##',            spaceName)
   userDataContents = userDataContents.replace('##user_data_machinetype##',      machinetypeName)
   userDataContents = userDataContents.replace('##user_data_machine_hostname##', hostName)
   userDataContents = userDataContents.replace('##user_data_manager_version##',  versionString)
   userDataContents = userDataContents.replace('##user_data_manager_hostname##', os.uname()[1])

   if machinefeaturesURL:
     userDataContents = userDataContents.replace('##user_data_machinefeatures_url##', machinefeaturesURL)

   if jobfeaturesURL:
     userDataContents = userDataContents.replace('##user_data_jobfeatures_url##', jobfeaturesURL)

   if joboutputsURL:
     userDataContents = userDataContents.replace('##user_data_joboutputs_url##', joboutputsURL)

   if rootImageURL:
     userDataContents = userDataContents.replace('##user_data_root_image_url##', rootImageURL)

   if heartbeatMachinesURL:
     userDataContents = userDataContents.replace('##user_data_heartbeat_machines_url##', heartbeatMachinesURL)

   # Deprecated vmtype/VM/VMLM terminology
   userDataContents = userDataContents.replace('##user_data_vmtype##',           machinetypeName)
   userDataContents = userDataContents.replace('##user_data_vm_hostname##',      hostName)
   userDataContents = userDataContents.replace('##user_data_vmlm_version##',     versionString)
   userDataContents = userDataContents.replace('##user_data_vmlm_hostname##',    os.uname()[1])

   if uuidStr:
     userDataContents = userDataContents.replace('##user_data_uuid##', uuidStr)

   # Insert a proxy created from user_data_proxy_cert / user_data_proxy_key
   if 'user_data_proxy' in options and options['user_data_proxy'] == True:
     certPath = machinetypePath + '/x509cert.pem'
     keyPath  = machinetypePath + '/x509key.pem'

     try:
       if ('legacy_proxy' in options) and options['legacy_proxy']:
         userDataContents = userDataContents.replace('##user_data_option_x509_proxy##',
                              makeX509Proxy(certPath, keyPath, shutdownTime, isLegacyProxy=True))
       else:
         userDataContents = userDataContents.replace('##user_data_option_x509_proxy##',
                              makeX509Proxy(certPath, keyPath, shutdownTime, isLegacyProxy=False, cn=machinetypeName))
     except Exception as e:
       raise VacutilsError('Faled to make proxy (' + str(e) + ')')

   # Site configurable substitutions for this machinetype
   for oneOption, oneValue in options.iteritems():
      if oneOption.startswith('user_data_option_'):
        userDataContents = userDataContents.replace('##' + oneOption + '##', oneValue)
      elif oneOption.startswith('user_data_file_'):
        try:
           if oneValue[0] == '/':
             f = open(oneValue, 'r')
           else:
             f = open(machinetypePath + '/files/' + oneValue, 'r')

           fileContents = f.read()
           f.close()

           # deprecated: replace ##user_data_file_xxxx## with value
           userDataContents = userDataContents.replace('##' + oneOption + '##', fileContents)

           # new behaviour: replace ##user_data_option_xxxx## with value from user_data_file_xxxx
           userDataContents = userDataContents.replace('##user_data_option_' + oneOption[15:] + '##', fileContents)

        except:
           raise VacutilsError('Failed to read ' + oneValue + ' for ' + oneOption)

   # Remove any unused patterns from the template
   userDataContents = re.sub('##user_data_[a-z,0-9,_]*##', '', userDataContents)

   return userDataContents

def emptyCallback1(p1):
   return

def emptyCallback2(p1, p2):
   return

def makeX509Proxy(certPath, keyPath, expirationTime, isLegacyProxy=False, cn=None):
   # Return a PEM-encoded limited proxy as a string in either Globus Legacy
   # or RFC 3820 format. Checks that the existing cert/proxy expires after
   # the given expirationTime, but no other checks are done.

   # First get the existing priviate key

   try:
     oldKey = M2Crypto.RSA.load_key(keyPath, emptyCallback1)
   except Exception as e:
     raise VacutilsError('Failed to get private key from ' + keyPath + ' (' + str(e) + ')')

   # Get the chain of certificates (just one if a usercert or hostcert file)

   try:
     certBIO = M2Crypto.BIO.File(open(certPath))
   except Exception as e:
     raise VacutilsError('Failed to open certificate file ' + certPath + ' (' + str(e) + ')')

   oldCerts = []

   while True:
     try:
       oldCerts.append(M2Crypto.X509.load_cert_bio(certBIO))
     except:
       certBIO.close()
       break

   if len(oldCerts) == 0:
     raise VacutilsError('Failed get certificate from ' + certPath)

   # Check the expirationTime

   if int(calendar.timegm(time.strptime(str(oldCerts[0].get_not_after()), "%b %d %H:%M:%S %Y %Z"))) < expirationTime:
     raise VacutilsError('Cert/proxy ' + certPath + ' expires before given expiration time ' + str(expirationTime))

   # Create the public/private keypair for the new proxy

   newKey = M2Crypto.EVP.PKey()
   newKey.assign_rsa(M2Crypto.RSA.gen_key(1024, 65537, emptyCallback2))

   # Start filling in the new certificate object

   newCert = M2Crypto.X509.X509()
   newCert.set_pubkey(newKey)
   newCert.set_serial_number(int(time.time() * 100))
   newCert.set_issuer_name(oldCerts[0].get_subject())
   newCert.set_version(2) # "2" is X.509 for "v3" ...

   # Construct the legacy or RFC style subject

   newSubject = oldCerts[0].get_subject()

   if isLegacyProxy:
     # Globus legacy proxy
     newSubject.add_entry_by_txt(field = "CN",
                                 type  = 0x1001,
                                 entry = 'limited proxy',
                                 len   = -1,
                                 loc   = -1,
                                 set   = 0)
   elif cn:
     # RFC proxy, probably with machinetypeName as proxy CN
     newSubject.add_entry_by_txt(field = "CN",
                                 type  = 0x1001,
                                 entry = cn,
                                 len   = -1,
                                 loc   = -1,
                                 set   = 0)
   else:
     # RFC proxy, with Unix time as CN
     newSubject.add_entry_by_txt(field = "CN",
                                 type  = 0x1001,
                                 entry = str(int(time.time() * 100)),
                                 len   = -1,
                                 loc   = -1,
                                 set   = 0)

   newCert.set_subject_name(newSubject)

   # Set start and finish times

   newNotBefore = M2Crypto.ASN1.ASN1_UTCTIME()
   newNotBefore.set_time(int(time.time()))
   newCert.set_not_before(newNotBefore)

   newNotAfter = M2Crypto.ASN1.ASN1_UTCTIME()
   newNotAfter.set_time(expirationTime)
   newCert.set_not_after(newNotAfter)

   # Add extensions, possibly including RFC-style proxyCertInfo

   newCert.add_ext(M2Crypto.X509.new_extension("keyUsage", "Digital Signature, Key Encipherment, Key Agreement", 1))

   if not isLegacyProxy:
     newCert.add_ext(M2Crypto.X509.new_extension("proxyCertInfo", "critical, language:1.3.6.1.4.1.3536.1.1.1.9", 1, 0))

   # Sign the certificate with the old private key
   oldKeyEVP = M2Crypto.EVP.PKey()
   oldKeyEVP.assign_rsa(oldKey)
   newCert.sign(oldKeyEVP, 'sha256')

   # Return proxy as a string of PEM blocks

   proxyString = newCert.as_pem() + newKey.as_pem(cipher = None)

   for oneOldCert in oldCerts:
     proxyString += oneOldCert.as_pem()

   return proxyString

def getCernvmImageData(fileName):

   data = { 'verified' : False, 'dn' : None }

   try:
     length = os.stat(fileName).st_size
   except Exception as e:
     logLine('Failed to get CernVM image size (' + str(e) + ')')
     return data

   if length <= 65536:
     logLine('CernVM image only ' + str(length) + ' bytes long: must be more than 65536')
     return data

   try:
     f = open(fileName, 'r')
   except Exception as e:
     logLine('Failed to open CernVM image (' + str(e) + ')')
     return data

   try:
     f.seek(-64 * 1024, os.SEEK_END)
     metadataBlock = f.read(32 * 1024).rstrip("\x00")
     # Quick hack until the metadata section is fixed in the CernVM images (extra comma)
     metadataBlock = metadataBlock.replace('HEAD",\n', 'HEAD"\n')
     metadataDict  = json.loads(metadataBlock)

     if 'ucernvm-version' in metadataDict:
       data['version'] = metadataDict['ucernvm-version']
   except Exception as e:
     logLine('Failed to load Metadata Block JSON from CernVM image (' + str(e) + ')')

   try:
     f.seek(-32 * 1024, os.SEEK_END)
     signatureBlock = f.read(32 * 1024).rstrip("\x00")
     # Quick hack until the howto-verify section is fixed in the CernVM images (missing commas)
     signatureBlock = signatureBlock.replace('signature>"\n', 'signature>",\n').replace('cvm-sign01.cern.ch"\n', 'cvm-sign01.cern.ch",\n')
     signatureDict  = json.loads(signatureBlock)
   except Exception as e:
     logLine('Failed to load Signature Block JSON from CernVM image (' + str(e) + ')')
     return data

   try:
     f.seek(0, os.SEEK_SET)
     digestableImage = f.read(length - 32 * 1024)
     hash = hashlib.sha256(digestableImage)
     digest = hash.digest()
   except Exception as e:
     logLine('Failed to make digest of CernVM image (' + str(e) + ')')
     return data

   try:
     certificate = base64.b64decode(signatureDict['certificate'])
     x509 = M2Crypto.X509.load_cert_string(certificate)
     rsaPubkey = x509.get_pubkey().get_rsa()
   except Exception as e:
     logLine('Failed to get X.509 certificate and RSA public key (' + str(e) + ')')
     return data

   try:
     signature = base64.b64decode(signatureDict['signature'])
   except:
     logLine('Failed to get signature from CernVM Signature Block')
     return data

   if not rsaPubkey.verify(digest, signature, 'sha256'):
     logLine('Certificate and calculated hash do not match given signature')
     return data

   try:
     # This isn't provided by M2Crypto, so we use openssl command
     p = os.popen('/usr/bin/openssl verify -CApath /etc/grid-security/certificates >/dev/null', 'w')
     p.write(certificate)

     if p.close() is None:
       try:
         dn = str(x509.get_subject())
       except Exception as e:
         logLine('Failed to get X.509 Subject DN (' + str(e) + ')')
         return data
       else:
         data['verified'] = True
         data['dn']       = dn

   except Exception as e:
     logLine('Failed to run /usr/bin/openssl verify command (' + str(e) + ')')
     return data

   return data

def getRemoteRootImage(url, imageCache, tmpDir, versionString):

   try:
     f, tempName = tempfile.mkstemp(prefix = 'getRemoteRootImage-', dir = tmpDir)
   except Exception as e:
     VacutilsError('Failed to create temporary image file in ' + tmpDir)

   ff = os.fdopen(f, 'wb')

   c = pycurl.Curl()
   c.setopt(c.USERAGENT, versionString)
   c.setopt(c.URL, url)
   c.setopt(c.WRITEDATA, ff)

   urlEncoded = urllib.quote(url,'')

   try:
     # For existing files, we get the mtime and only fetch the image itself if newer.
     # We check mtime not ctime since we will set it to remote Last-Modified: once downloaded
     c.setopt(c.TIMEVALUE, int(os.stat(imageCache + '/' + urlEncoded).st_mtime))
     c.setopt(c.TIMECONDITION, c.TIMECONDITION_IFMODSINCE)
   except:
     pass

   c.setopt(c.TIMEOUT, 120)

   # You will thank me for following redirects one day :)
   c.setopt(c.FOLLOWLOCATION, 1)
   c.setopt(c.OPT_FILETIME,   1)
   c.setopt(c.SSL_VERIFYPEER, 1)
   c.setopt(c.SSL_VERIFYHOST, 2)

   if os.path.isdir('/etc/grid-security/certificates'):
     c.setopt(c.CAPATH, '/etc/grid-security/certificates')
   else:
     logLine('/etc/grid-security/certificates directory does not exist - relying on curl bundle of commercial CAs')

   logLine('Checking if an updated ' + url + ' needs to be fetched')

   try:
     c.perform()
     ff.close()
   except Exception as e:
     os.remove(tempName)
     raise VacutilsError('Failed to fetch ' + url + ' (' + str(e) + ')')

   if c.getinfo(c.RESPONSE_CODE) == 200:
     try:
       lastModified = float(c.getinfo(c.INFO_FILETIME))
     except:
       os.remove(tempName)
       # We fail rather than use a server that doesn't give Last-Modified:
       raise VacutilsError('Failed to get last modified time for ' + url)

     if lastModified < 0.0:
       os.remove(tempName)
       # We fail rather than use a server that doesn't give Last-Modified:
       raise VacutilsError('Failed to get last modified time for ' + url)
     else:
       # We set mtime to Last-Modified: in case our system clock is very wrong, to prevent
       # continually downloading the image based on our faulty filesystem timestamps
       os.utime(tempName, (time.time(), lastModified))

     try:
       os.rename(tempName, imageCache + '/' + urlEncoded)
     except:
       try:
         os.remove(tempName)
       except:
         pass

       raise VacutilsError('Failed renaming new image ' + imageCache + '/' + urlEncoded)

     logLine('New ' + url + ' put in ' + imageCache)

   else:
     logLine('No new version of ' + url + ' found and existing copy not replaced')
     os.remove(tempName)

   c.close()
   return imageCache + '/' + urlEncoded

def splitCommaHeaders(inputList):

   outputList = []

   for x in inputList:

     if ',' in x:
       for y in re.split(r', *', x):
         outputList.append(y.strip())
     else:
       outputList.append(x.strip())

   return outputList

def setProcessName(processName):

   try:
     # Load the libc symbols
     libc = ctypes.cdll.LoadLibrary('libc.so.6')

     # Set up the C-style string
     s = ctypes.create_string_buffer(len(processName) + 1)
     s.value = processName

     # PR_SET_NAME=15 in /usr/include/linux/prctl.h
     libc.prctl(15, ctypes.byref(s), 0, 0, 0)

   except:
     logLine('Failed setting process name to ' + processName + ' using prctl')
     return

   try:
     # Now find argv[] so we can overwrite it too
     argc_t = ctypes.POINTER(ctypes.c_char_p)

     Py_GetArgcArgv = ctypes.pythonapi.Py_GetArgcArgv
     Py_GetArgcArgv.restype = None
     Py_GetArgcArgv.argtypes = [ctypes.POINTER(ctypes.c_int), ctypes.POINTER(argc_t)]

     argv = ctypes.c_int(0)
     argc = argc_t()
     Py_GetArgcArgv(argv, ctypes.pointer(argc))

     # Count up the available space
     currentSize = -1

     for oneArg in argc:
       try:
         # start from -1 to cancel the first "+ 1"
         currentSize += len(oneArg) + 1
       except:
         break

     # Cannot write more than already there
     if len(processName) > currentSize:
       processName = processName[:currentSize]

     # Zero what is already there
     ctypes.memset(argc.contents, 0, currentSize + 1)

     # Write in the new process name
     ctypes.memmove(argc.contents, processName, len(processName))

   except:
     logLine('Failed setting process name in argv[] to ' + processName)
     return

def makeSyncRecord(dirPrefix, targetYearMonth, tmpDir):

   try:
      targetMonth = int(targetYearMonth[4:6])
      targetYear  = int(targetYearMonth[0:4])
   except:
      print 'Cannot parse as YYYYMM: ' + targetYearMonth
      return 1

   numberJobs = 0
   site       = None
   submitHost = None

   recordsList = glob.glob(dirPrefix + '/apel-archive/' + targetYearMonth + '*/*')
   # We go backwards in time, assuming that site and SubmitHost for
   # the most recent record are correct
   recordsList.sort(reverse=True)

   for fileName in recordsList:
      thisSite = None
      thisSubmitHost = None

      for line in open(fileName, 'r'):
        if line.startswith('Site:'):
          thisSite = line[5:].strip()
        elif line.startswith('SubmitHost:'):
          thisSubmitHost = line[11:].strip()

        if thisSite and thisSubmitHost:
          break

      if thisSite is None:
        print 'No Site given in ' + fileName + ' !! - please fix this - skipping'
        continue

      if thisSubmitHost is None:
        print 'No SubmitHost given in ' + fileName + ' !! - please fix this - skipping'
        continue

      if site is None:
        site = thisSite
      elif site != thisSite:
        print 'Site changes from ' + site + ' to ' + thisSite + ' - please fix ' + fileName + ' - skipping'
        continue

      if submitHost is None:
        submitHost = thisSubmitHost
      elif submitHost != thisSubmitHost:
        print 'SubmitHost changes from ' + submitHost + ' to ' + thisSubmitHost + ' - please fix ' + fileName + ' - skipping'
        continue

      numberJobs += 1

   syncRecord = 'APEL-sync-message: v0.1\n'               \
                'Site: ' + site + '\n'                    \
                'SubmitHost: ' + submitHost + '\n'        \
                'NumberOfJobs: ' + str(numberJobs) + '\n' \
                'Month: ' + str(targetMonth) + '\n'       \
                'Year: ' + str(targetYear) + '\n'         \
                '%%\n'

   gmtime = time.gmtime()

   try:
     os.makedirs(time.strftime(dirPrefix + '/apel-outgoing/%Y%m%d', gmtime))
   except:
     pass

   syncFileName = time.strftime(dirPrefix + '/apel-outgoing/%Y%m%d/%H%M%S', gmtime) + (str(time.time() % 1) + '00000000')[2:10]

   if createFile(syncFileName, syncRecord, stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH, tmpDir):
      print 'Created ' + syncFileName
      return 0

   print 'Failed to create ' + syncFileName
   return 2


def makeSshFingerprint(pubFileLine):
   # Convert a line from an ssh id_rsa.pub (or id_dsa.pub) file to a fingerprint

   try:
     fingerprint = hashlib.md5(base64.b64decode(pubFileLine.strip().split()[1].encode('ascii'))).hexdigest()
     return ':'.join(fingerprint[i:i+2] for i in range(0, len(fingerprint), 2))
   except:
     return None

def loadAvg(which = None):
   # By default, use maximum load average
   # which = 0, 1, or 2

   try:
     load0,load1,load2 = open('/proc/loadavg').readline().split()[0:3]
     loadList = [float(load0),float(load1),float(load2)]
   except Exception as e:
     print 'Failed to parse /proc/loadavg (' + str(e) + ')'
     return None

   if which is None:
     return max(loadList)
   else:
     return loadList[which]

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

def updateSpaceInGOCDB(siteName, spaceName, serviceType, certPath, keyPath, caPath, versionString, spaceValues, machinetypesValues):

   id            = None
   keys          = {}
   machinetypes  = {}
   curl          = pycurl.Curl()

   # First get the current state from GOCDB

   buffer = StringIO.StringIO()
   curl.setopt(curl.WRITEFUNCTION, buffer.write)
   curl.setopt(curl.USERAGENT, versionString)
   curl.setopt(curl.TIMEOUT, 30)
   curl.setopt(curl.FOLLOWLOCATION, True)
   curl.setopt(curl.SSL_VERIFYPEER, 1)
   curl.setopt(curl.SSL_VERIFYHOST, 2)
   curl.setopt(curl.SSLCERT, certPath)
   curl.setopt(curl.SSLKEY, keyPath)
   curl.setopt(curl.CAPATH, caPath)
   curl.setopt(curl.URL, 'https://goc.egi.eu/gocdbpi/public/?method=get_service_endpoint&sitename=%s&hostname=%s&service_type=%s' % (siteName, spaceName, serviceType))

   try:
     curl.perform()
   except Exception as e:
     raise VacutilsError('Failed to read service data (' + str(e) + ')')

   try:
     serviceEndpoint = xml.etree.cElementTree.fromstring(buffer.getvalue()).find('SERVICE_ENDPOINT')
   except Exception as e:
     raise VacutilsError('Failed to extract serviceEndpoint (' + str(e) + ')')

   try:
      id = serviceEndpoint.attrib['PRIMARY_KEY'].split('G')[0]

      for i in serviceEndpoint:

        if i.tag == 'EXTENSIONS':
          for ext in i:
            keys[ext.find('KEY').text] = ext.find('VALUE').text

        elif i.tag == 'ENDPOINTS':

          for endpoint in i:
            endpointDict = {}
            endpointName = None

            for j in endpoint:

              if j.text is None:
                endpointDict[j.tag] = j.text

              elif j.tag == 'EXTENSIONS':
                for ext in j:
                  endpointDict[ext.find('KEY').text] = ext.find('VALUE').text

              elif j.tag == 'NAME':
                endpointName = j.text.strip()

              else:
                endpointDict[j.tag] = j.text.strip()

            if endpointName:
              machinetypes[endpointName] = endpointDict

        elif i.text is None:
          keys[i.tag] = None

        else:
          keys[i.tag] = i.text.strip()

   except Exception as e:
      raise VacutilsError('Problem parsing XML tree (' + str(e) + ')')

   # Now send the updates: service extensions first
   curl.setopt(curl.CUSTOMREQUEST, 'PUT')
   curl.setopt(curl.POSTFIELDS, json.dumps(spaceValues))
   curl.setopt(curl.URL, 'https://goc.egi.eu/gocdbpi/v5/Service/%s/ExtensionProperties' % id)

   try:
     curl.perform()
   except Exception as e:
     raise VacutilsError('Failed to update service data (' + str(e) + ')')

   if curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
     raise VacutilsError('PUT %s fails with HTTP code %d!' % (curl.URL, curl.getinfo(pycurl.RESPONSE_CODE)))

# WE DON'T DO THIS YET SINCE GOCDB DOES NOT SUPPORT CREATING ENDPOINTS THROUGH THE API!
#
#   # Next the endpoint
#
#   for machinetypeName in machinetypesValues:
#     # need to get the endpointId from machinetypes
#
#     try:
#       endpointID = machinetypes[machinetypeName]['ID']
#     except:
#       raise VacutilsError('Cannot get endpoint ID for %s' % machinetypeName)
#
#     print endpointID,machinetypeName,machinetypesValues[machinetypeName]
#
#     curl.setopt(curl.POSTFIELDS, json.dumps(machinetypesValues[machinetypeName]))
#     curl.setopt(curl.URL, 'https://goc.egi.eu/gocdbpi/v5/EndPoint/%s/ExtensionProperties' % endpointID )
#
#     try:
#       curl.perform()
#     except Exception as e:
#       raise VacutilsError('Failed to update service data (' + str(e) + ')')

