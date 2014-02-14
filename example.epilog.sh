#!/bin/sh
#
# This is run near the end of the boot process, after
# networking has come up.

echo 'epilog.sh file has started' >>/tmp/epilog.sh.log

sleep 600

if [ -r /etc/machinefeatures/shutdown_command ] ; then
  ShutdownCommand=`cat /etc/machinefeatures/shutdown_command`
fi
  
if [ -x "$ShutdownCommand" ] ; then
  $ShutdownCommand 200 Finished
else
  shutdown -h now
fi
      
sleep 1234567890
