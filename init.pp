############################################################
###  Puppet Module for Vac 03.00 or later on CentOS 7.x  ###
############################################################
#
# This file, init.pp, is sufficient to create a Vac module in your Puppet
# set up. It should be installed in the modules part of your tree:
#
#   ../modules/vac/manifests/init.pp
#
# The original version of this file is distributed along with Vac and is 
# included in the /usr/share/doc/vac-VERSION directory when Vac is installed.
#
# The module takes static segments of Vac configuration file and installs
# them in /etc/vac.d which Vac will then assemble in memory at the start of 
# each cycle (roughly once a minute.) Hosts and groups of hosts can select
# which configuration file segments to receive when they invoke the vac class,
# using its parameters space, subspace, and subspace1-subspace9. If multiple
# versions of a segment exist, then the most specific will be installed.
# Implicitly, host-specific and site-wide versions may also be used. 
#
# These configuration file segments must appear within the puppet
# fileserver's tree, with the location indicated by the etc_path parameter
# of the class. In the simplest case, a files subdirectory can be included
# when installing the vac module in Puppet which the default etc_path will 
# point to. With etc_path='modules/vac/vac.d', the tree would be like:
#
#  puppet:///modules/vac/vac.d/HOSTNAME/
#  puppet:///modules/vac/vac.d/SUBSPACE/
#  puppet:///modules/vac/vac.d/SUBSPACE1/
#  ...
#  puppet:///modules/vac/vac.d/SUBSPACE9/
#  puppet:///modules/vac/vac.d/SPACE/
#  puppet:///modules/vac/vac.d/site/
#
# where HOSTNAME, SUBSPACE, SUBSPACE1-9, SPACE are specific to the host.
#
# These directories are searched in turn for configuration files, with
# the first one found with each name being used in case of conflicts.
#
# This module does not install or manage the file /etc/vac.conf which Vac
# reads last. This allows you to manually override the options from Puppet's 
# configuration file segments on a particular machine for testing and 
# debugging, without your changes being continually reverted by Puppet.
# 
# In most cases, it will be sufficient to create and populate the directory
# for the space(s) at the site and leave all the other directories empty 
# (or not created).
#
#
# A similar mechanism is used to install the files for machinetypes. Typically, 
# these will be hostcert.pem and hostkey.pem and perhaps a static user_data
# file. The machinetypes_path parameter of the class allows you to specify a
# different path on the Puppet fileserver to the default 'modules/vac/machinetypes',
# the machinetypes subdirectory of the Vac module's files directory. In the default
# case, the tree looks like:
# 
#  puppet:///modules/vac/machinetypes/HOSTNAME/
#  puppet:///modules/vac/machinetypes/SUBSPACE/
#  puppet:///modules/vac/machinetypes/SUBSPACE1/
#  ...
#  puppet:///modules/vac/machinetypes/SUBSPACE9/
#  puppet:///modules/vac/machinetypes/SPACE/
#  puppet:///modules/vac/machinetypes/site/
#
# This is similar to the configuration files tree, but instead of these
# directories containing files they must contain machinetype directories 
# suitable for installing as a machinetype subdirectory tree of 
# /var/lib/vac/machinetypes/ . As above, the first machinetype
# directory tree found with a given name is the one which will be installed.
# 
# 
# For both configuration and machinetype files, you may want to create dedicated
# areas of the Puppet fileserver tree outside of the vac module, either as
# local modules or by directly inserting them. You can use the etc_path and
# machinetypes_path parameters to point to your dedicated trees.
#
# Nagios/NRPE: 
# In addition to the configuration and machinetype files, by setting nagios_nrpe
# to true you can install a Vac file in /etc/nrpe.d to enable Nagios NRPE
# monitoring using Vac's check-vacd script. This will ensure the nrpe RPM
# is installed and will restart the nrpe service to reread the configuration.
# This assumes that you have an nrpe module already or have declared nrpe
# as a service.
#
# APEL:
# If you give the apel_cert_path and apel_key_path parameters when invoking the 
# class, the APEL ssmsend client will be run each hour from cron to send usage data
# to the production APEL service. The path parameters must be paths on the Puppet 
# fileserver (without the leading puppet:///). 
# YOU MUST AGREE USE OF APEL WITH THE APEL TEAM BEFORE STARTING TO USE APEL
#
# SQUID:
# If you set local_squid to true, Puppet will install and configure a Squid cache
# for cvmfs on the factory, using the /etc/squid/squid.conf.vac template installed
# by the vac RPM. The proxy is visible from within the VMs at 169.254.169.253:3128
#
# Andrew.McNab@cern.ch  March  2016  http://www.gridpp.ac.uk/vac/
#

#
class vac ($space              = "vac01.${domain}",
           $subspace           = '',
           $subspace1          = '',
           $subspace2          = '',
           $subspace3          = '',
           $subspace4          = '',
           $subspace5          = '',
           $subspace6          = '',
           $subspace7          = '',
           $subspace8          = '',
           $subspace9          = '',
           $etc_path           = 'modules/vac/vac.d',
           $machinetypes_path  = 'modules/vac/machinetypes',
           $nagios_nrpe        = false,
           $apel_bdii_hostport = '',
           $apel_cert_path     = '',
           $apel_key_path      = '',
           $local_squid        = false)
{
  #
  # Install site-wide or increasingly specific configuration files in /etc/vac.d
  #
  file { '/etc/vac.d':
         require => Package['vac'],
         ensure  => directory,
         recurse => true,
         purge   => true,
         force   => true,
         owner   => 'root',
         group   => 'root',
         mode    => 0644,
         sourceselect => 'all',
         source  => [ 
                      "puppet:///${etc_path}/${fqdn}",
                      # we could use arrays for subspaces but it would be messy
                      "puppet:///${etc_path}/${space}:${subspace}",
                      "puppet:///${etc_path}/${space}:${subspace1}",
                      "puppet:///${etc_path}/${space}:${subspace2}",
                      "puppet:///${etc_path}/${space}:${subspace3}",
                      "puppet:///${etc_path}/${space}:${subspace4}",
                      "puppet:///${etc_path}/${space}:${subspace5}",
                      "puppet:///${etc_path}/${space}:${subspace6}",
                      "puppet:///${etc_path}/${space}:${subspace7}",
                      "puppet:///${etc_path}/${space}:${subspace8}",
                      "puppet:///${etc_path}/${space}:${subspace9}",
                      "puppet:///${etc_path}/${space}",
                      "puppet:///${etc_path}/site",
                    ],
       }

  #
  # Install site-wide or increasingly specific machinetype files (probably hostcert.pem
  # and hostkey.pem) under /var/lib/vac/machinetypes/...
  #
  file { '/var/lib/vac/machinetypes':
         require => Package['vac'],
         ensure  => directory,
         recurse      => true,
         purge        => true,
         force        => true,
         owner        => 'root',
         group        => 'root',
         sourceselect => 'all',
         source  => [ 
                      "puppet:///${machinetypes_path}/${fqdn}",
                      # we could use arrays for subspaces but it would be messy
                      "puppet:///${machinetypes_path}/${space}:${subspace}",
                      "puppet:///${machinetypes_path}/${space}:${subspace1}",
                      "puppet:///${machinetypes_path}/${space}:${subspace2}",
                      "puppet:///${machinetypes_path}/${space}:${subspace3}",
                      "puppet:///${machinetypes_path}/${space}:${subspace4}",
                      "puppet:///${machinetypes_path}/${space}:${subspace5}",
                      "puppet:///${machinetypes_path}/${space}:${subspace6}",
                      "puppet:///${machinetypes_path}/${space}:${subspace7}",
                      "puppet:///${machinetypes_path}/${space}:${subspace8}",
                      "puppet:///${machinetypes_path}/${space}:${subspace9}",
                      "puppet:///${machinetypes_path}/${space}",
                      "puppet:///${machinetypes_path}/site",
                    ],
       }

  #
  # Vac RPM itself, which pulls in other RPMs via Requires:
  #
  package { 'vac':
            ensure  => 'installed',
          }

  #
  # Daemons we depend on
  #
  service { 'libvirtd':
             enable => true,
             ensure => "running",
          }
  service { 'vacd':
             enable => true,
             ensure => "running",
          }
  exec    { 'overcommit_memory':
            command => '/bin/echo 1 > /proc/sys/vm/overcommit_memory',
            unless  => '/usr/bin/test `/bin/cat /proc/sys/vm/overcommit_memory` = 1'
          }
  service { 'numad':
             enable => true,
             ensure => "running",
          }

# This caused instability with 2.6.32-642 kernels?
  exec    { 'unset_merge_across_nodes':
            command => '/bin/echo 2 > /sys/kernel/mm/ksm/run; /bin/echo 0 > /sys/kernel/mm/ksm/merge_across_nodes; /bin/echo 1 > /sys/kernel/mm/ksm/run',
            unless  => '/usr/bin/test `/bin/cat /sys/kernel/mm/ksm/merge_across_nodes` = 0',
            before  => Service['ksm'],
          }
  service { 'ksm':
             enable => true,
             ensure => "running",
             require => Service['numad'],
          }
  service { 'ksmtuned':
             enable  => true,
             ensure  => "running",
             require => Service['ksm'],
          }

  file { '/etc/ksmtuned.conf':
         ensure  => 'file',
         content => "LOGFILE=/var/log/ksmtuned\nDEBUG=1\nKSM_THRES_COEF=66\n",
         owner   => 'root',
         group   => 'root',
         mode    => '0644',
         notify  => Service['ksmtuned'],
       }

  file { '/etc/logrotate.d/ksmtuned':
         ensure  => 'file',
         content => "/var/log/ksmtuned\n{\ndaily\nmissingok\nrotate 3\n}\n",
         owner   => 'root',
         group   => 'root',
         mode    => '0644',
       }

  # "Network As A Service"!
  # This allows us to restart networking after making changes
  service { 'network': }

  #
  # Use /etc/sysconfig/pcmcia to hand NOZEROCONF=yes to /etc/rc.d/init.d/network
  # (Not needed for CentOS 7?)
  #
  file { '/etc/sysconfig/pcmcia':
         ensure  => 'file',
         content => "# We use this for local additions to /etc/sysconfig/network\nexport NOZEROCONF=yes\n",
         owner   => 'root',
         group   => 'root',
         mode    => '0644',
         notify  => Service['network'],
         before  => Service['vacd'],
       }

  #
  # Make sure root has a key pair, which is used to login to VMs
  #
  exec { "create_id_rsa":
         command => "/usr/bin/ssh-keygen -q -N '' -f /root/.ssh/id_rsa",
         creates => "/root/.ssh/id_rsa.pub",
       }

  #
  # Run check-vacd from Nagios NRPE
  #
  if $nagios_nrpe
    {
      file { '/etc/nrpe.d/nagios-plugins-check-vacd.cfg':
             ensure  => 'file',
             content => "# Use Vac's script to check status\ncommand[check-vacd]=/usr/sbin/check-vacd 600\n",
             require => Package['nrpe','vac'],
             owner   => 'root',
             group   => 'root',
             mode    => '0644',
             notify  => Service['nrpe'],
           }
    }
  else # uninstall if not set true
    {
      file { '/etc/nrpe.d/nagios-plugins-check-vacd.cfg':
             ensure  => 'absent',
           }      
    }

  #
  # Install files used by the ssmsend cron installed by Vac RPM
  #
  if ($apel_cert_path != '') and ($apel_key_path != '')
    {
      package { 'apel-ssm':
                ensure  => 'installed',
              }

      file { '/etc/grid-security/vac-apel-cert.pem':
             ensure  => 'file',
             source  => "puppet:///${apel_cert_path}",
             owner   => 'root',
             group   => 'root',
             mode    => '0644',
           }

      file { '/etc/grid-security/vac-apel-key.pem':
             ensure  => 'file',
             source  => "puppet:///${apel_key_path}",
             owner   => 'root',
             group   => 'root',
             mode    => '0600',
           }
    }

  #
  # Configure files used by the local squid on this factory
  #
  if $local_squid
    {
      package { 'squid':
                ensure  => 'installed',
                notify => Exec['make_squid_conf'],
              }

      service { 'squid':
                enable  => true,
                ensure  => "running",
                require => Package['squid','vac'],
                subscribe => File['/etc/squid/squid.conf'],
              }

      exec    { 'make_squid_conf':
                command => "/usr/sbin/vac squid-conf /etc/squid/squid.conf.vac /etc/squid/squid.conf",
                require => Package['squid','vac'],
                before => File['/etc/squid/squid.conf'],
              }

      file    { "/etc/squid/squid.conf":
                audit => content,
              }
    }
}
