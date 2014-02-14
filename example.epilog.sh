#!/bin/sh
#
# This is run near the end of the boot process, after
# networking has come up.

echo 'epilog.sh file has started' >>/tmp/epilog.sh.log

sleep 600

if [ -r /etc/vmtypefiles/shutdown_command ] ; then
  ShutdownCommand=`cat /etc/vmtypefiles/shutdown_command`
fi
  
if [ "$ShutdownCommand" ] ; then
  $ShutdownCommand 200 Finished
else
  shutdown -h now
fi
      
sleep 1234567890
