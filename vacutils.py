#
#  vacutils.py - reusable functions and classes for Vac and Vcycle
#
## THE DEFINITIVE SOURCE OF THIS FILE IS THE vac GIT REPOSITORY ##
##   NEW VERSIONS ARE COPIED INTO THE Vcycle REPO AS NECESSARY  ##
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
import stat
import time
import string
import urllib
import StringIO
import tempfile

import pycurl

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
   except:
     return False

def secondsToHHMMSS(seconds):
   hh, ss = divmod(seconds, 3600)
   mm, ss = divmod(ss, 60)
   return '%02d:%02d:%02d' % (hh, mm, ss)

def createUserData(vmtypesPath, options, versionString, spaceName, vmtypeName, userDataPath, hostName, uuidStr):
   
   # Get raw user_data template file, either from network ...
   if (userDataPath[0:7] == 'http://') or (userDataPath[0:8] == 'https://'):
     buffer = StringIO.StringIO()
     c = pycurl.Curl()
     c.setopt(c.URL, userDataPath)
     c.setopt(c.WRITEFUNCTION, buffer.write)
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
       userDataFile = vmtypesPath + '/' + vmtypeName + '/' + userDataPath

     try:
       u = open(userDataFile, 'r')
       userDataContents = u.read()
       u.close()
     except:
       raise NameError('Failed to read ' + userDataFile)

   # Default substitutions
   userDataContents = userDataContents.replace('##user_data_uuid##',          uuidStr)
   userDataContents = userDataContents.replace('##user_data_space##',         spaceName)
   userDataContents = userDataContents.replace('##user_data_vmtype##',        vmtypeName)
   userDataContents = userDataContents.replace('##user_data_vm_hostname##',   hostName)
   userDataContents = userDataContents.replace('##user_data_vmlm_version##',  versionString)
   userDataContents = userDataContents.replace('##user_data_vmlm_hostname##', os.uname()[1])

   # Site configurable substitutions for this vmtype
   for oneOption, oneValue in options.iteritems():
      if oneOption[0:17] == 'user_data_option_':
        userDataContents = userDataContents.replace('##' + oneOption + '##', oneValue)
      elif oneOption[0:15] == 'user_data_file_':
        try:
           if oneValue[0] == '/':
             f = open(oneValue, 'r')
           else:
             f = open(vmtypesPath + '/' + vmtypeName + '/' + oneValue, 'r')
                           
           userDataContents = userDataContents.replace('##' + oneOption + '##', f.read())
           f.close()
        except:
           raise NameError('Failed to read ' + oneValue + ' for ' + oneOption)          

   return userDataContents

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
