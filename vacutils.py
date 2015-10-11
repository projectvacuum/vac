#
#  vacutils.py - reusable functions and classes for Vac and Vcycle
#
## THE DEFINITIVE SOURCE OF THIS FILE IS THE Vac GIT REPOSITORY ##
## UNMODIFIED VERSIONS ARE COPIED TO THE Vcycle REPO AS NEEDED  ##
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
import re
import sys
import stat
import time
import string
import urllib
import StringIO
import tempfile
import calendar

import pycurl
import M2Crypto

def logLine(text):
   print time.strftime('%b %d %H:%M:%S [') + str(os.getpid()) + ']: ' + text
   sys.stdout.flush()

def createFile(targetname, contents, mode=stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP, tmpDir = None):
   # Create a temporary text file containing contents then move
   # it into place. Rename is an atomic operation in POSIX,
   # including situations where targetname already exists.

   if tmpDir is None:
     tmpDir = os.path.dirname(targetname)
   
   try:
     ftup = tempfile.mkstemp(prefix = 'temp', dir = tmpDir, text = True)
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

def createUserData(shutdownTime, machinetypesPath, options, versionString, spaceName, machinetypeName, userDataPath, hostName, uuidStr, 
                   machinefeaturesURL = None, jobfeaturesURL = None, joboutputsURL = None):
   
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
       raise NameError('Failed to read ' + userDataPath + ' (' + str(e) + ')')

     c.close()
     userDataContents = buffer.getvalue()

   # ... or from filesystem
   else:
     if userDataPath[0] == '/':
       userDataFile = userDataPath
     else:
       userDataFile = machinetypesPath + '/' + machinetypeName + '/' + userDataPath

     try:
       u = open(userDataFile, 'r')
       userDataContents = u.read()
       u.close()
     except:
       raise NameError('Failed to read ' + userDataFile)

   # Default substitutions
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

   # Deprecated vmtype/VM/VMLM terminology
   userDataContents = userDataContents.replace('##user_data_vmtype##',           machinetypeName)
   userDataContents = userDataContents.replace('##user_data_vm_hostname##',      hostName)
   userDataContents = userDataContents.replace('##user_data_vmlm_version##',     versionString)
   userDataContents = userDataContents.replace('##user_data_vmlm_hostname##',    os.uname()[1])

   if uuidStr:
     userDataContents = userDataContents.replace('##user_data_uuid##', uuidStr)

   # Insert a proxy created from user_data_proxy_cert / user_data_proxy_key
   if 'user_data_proxy_cert' in options and 'user_data_proxy_key' in options:

     if options['user_data_proxy_cert'][0] == '/':
       certPath = options['user_data_proxy_cert']
     else:
       certPath = machinetypesPath + '/' + machinetypeName + '/' + options['user_data_proxy_cert']

     if options['user_data_proxy_key'][0] == '/':
       keyPath = options['user_data_proxy_key']
     else:
       keyPath = machinetypesPath + '/' + machinetypeName + '/' + options['user_data_proxy_key']

     try:
       if ('legacy_proxy' in options) and options['legacy_proxy']:
         userDataContents = userDataContents.replace('##user_data_x509_proxy##',
                              makeX509Proxy(certPath, keyPath, shutdownTime, isLegacyProxy=True))
       else:
         userDataContents = userDataContents.replace('##user_data_x509_proxy##',
                              makeX509Proxy(certPath, keyPath, shutdownTime, isLegacyProxy=False))
     except Exception as e:
       raise NameError('Faled to make proxy (' + str(e) + ')')

   # Site configurable substitutions for this machinetype
   for oneOption, oneValue in options.iteritems():
      if oneOption[0:17] == 'user_data_option_':
        userDataContents = userDataContents.replace('##' + oneOption + '##', oneValue)
      elif oneOption[0:15] == 'user_data_file_':
        try:
           if oneValue[0] == '/':
             f = open(oneValue, 'r')
           else:
             f = open(machinetypesPath + '/' + machinetypeName + '/' + oneValue, 'r')
                           
           userDataContents = userDataContents.replace('##' + oneOption + '##', f.read())
           f.close()
        except:
           raise NameError('Failed to read ' + oneValue + ' for ' + oneOption)          

   # Remove any unused patterns from the template
   userDataContents = re.sub('##user_data_[a-z,0-9,_]*##', '', userDataContents)       
   
   return userDataContents

def emptyCallback1(p1):
   return

def emptyCallback2(p1, p2):
   return

def makeX509Proxy(certPath, keyPath, expirationTime, isLegacyProxy=False):
   # Return a PEM-encoded limited proxy as a string in either Globus Legacy 
   # or RFC 3820 format. Checks that the existing cert/proxy expires after
   # the given expirationTime, but no other checks are done.

   # First get the existing priviate key

   try:
     oldKey = M2Crypto.RSA.load_key(keyPath, emptyCallback1)
   except Exception as e:
     raise NameError('Failed to get private key from ' + keyPath + ' (' + str(e) + ')')

   # Get the chain of certificates (just one if a usercert or hostcert file)

   try:
     certBIO = M2Crypto.BIO.File(open(certPath))
   except Exception as e:
     raise NameError('Failed to open certificate file ' + certPath + ' (' + str(e) + ')')

   oldCerts = []

   while True:
     try:
       oldCerts.append(M2Crypto.X509.load_cert_bio(certBIO))
     except:
       certBIO.close()
       break
   
   if len(oldCerts) == 0:
     raise NameError('Failed get certificate from ' + certPath)

   # Check the expirationTime
   
   if int(calendar.timegm(time.strptime(str(oldCerts[0].get_not_after()), "%b %d %H:%M:%S %Y %Z"))) < expirationTime:
     raise NameError('Cert/proxy ' + certPath + ' expires before given expiration time ' + str(expirationTime))

   # Create the public/private keypair for the new proxy
   
   newKey = M2Crypto.EVP.PKey()
   newKey.assign_rsa(M2Crypto.RSA.gen_key(512, 65537, emptyCallback2))

   # Start filling in the new certificate object

   newCert = M2Crypto.X509.X509()
   newCert.set_pubkey(newKey)
   newCert.set_serial_number(int(time.time() * 100))
   newCert.set_issuer_name(oldCerts[0].get_subject())
   newCert.set_version(2) # "2" is X.509 for "v3" ...

   # Construct the legacy or RFC style subject

   newSubject = oldCerts[0].get_subject()

   if isLegacyProxy:
     newSubject.add_entry_by_txt(field = "CN",
                                 type  = 0x1001,
                                 entry = 'limited proxy',
                                 len   = -1, 
                                 loc   = -1, 
                                 set   = 0)
   else:
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

def getRemoteRootImage(url, imageCache, tmpDir):

   try:
     f, tempName = tempfile.mkstemp(prefix = 'tmp', dir = tmpDir)
   except Exception as e:
     NameError('Failed to create temporary image file in ' + tmpDir)
        
   ff = os.fdopen(f, 'wb')
   
   c = pycurl.Curl()
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
     raise NameError('Failed to fetch ' + url + ' (' + str(e) + ')')

   if c.getinfo(c.RESPONSE_CODE) == 200:
     try:
       lastModified = float(c.getinfo(c.INFO_FILETIME))
     except:
       # We fail rather than use a server that doesn't give Last-Modified:
       raise NameError('Failed to get last modified time for ' + url)

     if lastModified < 0.0:
       # We fail rather than use a server that doesn't give Last-Modified:
       raise NameError('Failed to get last modified time for ' + url)
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
           
       raise NameError('Failed renaming new image ' + imageCache + '/' + urlEncoded)

     logLine('New ' + url + ' put in ' + imageCache)

   else:
     logLine('No new version of ' + url + ' found and existing copy not replaced')
     
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
