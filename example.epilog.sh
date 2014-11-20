#!/bin/sh
#
# This is run near the end of the boot process, after
# networking has come up.

echo 'epilog.sh file has started' >>/tmp/epilog.sh.log

sleep 600

/sbin/shutdown -h now
      
sleep 1234567890
