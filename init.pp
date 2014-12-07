#########################################
###  Puppet Module for Vac on SL 6.x  ###
#########################################
#
# This file, init.pp, is sufficient to create a Vac module in your Puppet
# set up. It should be installed in the modules part of your tree:
#
#   ../modules/vac/manifests/init.pp
#
# The original version of this file is distributed along with Vac and is 
# included in the /var/lib/vac/doc directory when Vac is installed.
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
# A similar mechanism is used to install the files for vmtypes. Typically, 
# these will be hostcert.pem and hostkey.pem and perhaps a static user_data
# file. The vmtypes_path parameter of the class allows you to specify a
# different path on the Puppet fileserver to the default 'modules/vac/vmtypes',
# the vmtypes subdirectory of the Vac module's files directory. In the default
# case, the tree looks like:
# 
#  puppet:///modules/vac/vmtypes/HOSTNAME/
#  puppet:///modules/vac/vmtypes/SUBSPACE/
#  puppet:///modules/vac/vmtypes/SUBSPACE1/
#  ...
#  puppet:///modules/vac/vmtypes/SUBSPACE9/
#  puppet:///modules/vac/vmtypes/SPACE/
#  puppet:///modules/vac/vmtypes/site/
#
# This is similar to the configuration files tree, but instead of these
# directories containing files they must contain vmtype directories suitable
# for installing below /var/lib/vac/vmtypes/ . As above, the first vmtype
# directory found with a given name is the one which will be installed.
# 
# 
# For both configuration and vmtype files, you may want to create dedicated 
# areas of the Puppet fileserver tree outside of the vac module, either as
# local modules or by directly inserting them. You can use the etc_path and
# vmtypes_path parameters to point to your dedicated trees.
#
# Nagios/NRPE: 
# In addition to the configuration and vmtype files, by setting nagios_nrpe
# to true you can install a Vac file in /etc/nrpe.d to enable Nagios NRPE
# monitoring using Vac's check-vacd script. This will ensure the nrpe RPM
# is installed and will restart the nrpe service to reread the configuration.
# This assumes that you have an nrpe module already or have declared nrpe
# as a service.
#
# APEL:
# If you give the apel_cert_path and apel_key_path parameters when invoking
# the class, the APEL ssmsend client will be run each hour from cron to
# send usage data to the production APEL service. The two path parameters
# must be full paths on the Puppet fileserver starting with puppet:/// .
# YOU MUST AGREE USE OF APEL WITH THE APEL TEAM BEFORE STARTING TO USE APEL
#
# Andrew.McNab@cern.ch  December 2014  http://www.gridpp.ac.uk/vac/
#

#
class vac ($space          = "vac01.${domain}",
           $subspace       = '',
           $subspace1      = '',
           $subspace2      = '',
           $subspace3      = '',
           $subspace4      = '',
           $subspace5      = '',
           $subspace6      = '',
           $subspace7      = '',
           $subspace8      = '',
           $subspace9      = '',
           $etc_path       = 'modules/vac/vac.d',
           $vmtypes_path   = 'modules/vac/vmtypes',
           $nagios_nrpe    = false,
           $apel_cert_path = '',
           $apel_key_path  = '')
{
  #
  # Install site-wide or increasingly specific configuration files in /etc/vac.d
  #
  file { '/etc/vac.d':
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
  # Install site-wide or increasingly specific vmtype files (probably hostcert.pem
  # and hostkey.pem) under /var/lib/vac/vmtypes/...
  #
  file { '/var/lib/vac/vmtypes':
         ensure  => directory,
         recurse      => true,
         purge        => true,
         force        => true,
         owner        => 'root',
         group        => 'root',
         sourceselect => 'all',
         source  => [ 
                      "puppet:///${vmtypes_path}/${fqdn}",
                      # we could use arrays for subspaces but it would be messy
                      "puppet:///${vmtypes_path}/${space}:${subspace}",
                      "puppet:///${vmtypes_path}/${space}:${subspace1}",
                      "puppet:///${vmtypes_path}/${space}:${subspace2}",
                      "puppet:///${vmtypes_path}/${space}:${subspace3}",
                      "puppet:///${vmtypes_path}/${space}:${subspace4}",
                      "puppet:///${vmtypes_path}/${space}:${subspace5}",
                      "puppet:///${vmtypes_path}/${space}:${subspace6}",
                      "puppet:///${vmtypes_path}/${space}:${subspace7}",
                      "puppet:///${vmtypes_path}/${space}:${subspace8}",
                      "puppet:///${vmtypes_path}/${space}:${subspace9}",
                      "puppet:///${vmtypes_path}/${space}",
                      "puppet:///${vmtypes_path}/site",
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
  service { "vacd":
             enable => true,
             ensure => "running",
          }
  service { "nfs":
             enable => true,
             ensure => "running",
          }
  service { "ksm":
             enable => true,
             ensure => "running",
          }
  service { "ksmtuned":
             enable => true,
             ensure => "running",
          }

  # "Network As A Service"!
  # This allows us to restart networking after making changes
  service { 'network': }

  #
  # Use /etc/sysconfig/pcmcia to hand NOZEROCONF=yes to /etc/rc.d/init.d/network
  #
  file { '/etc/sysconfig/pcmcia':
         ensure  => 'file',
         content => "# We use this for local additions to /etc/sysconfig/network\nexport NOZEROCONF=yes\n",
         owner   => 'root',
         group   => 'root',
         mode    => '0644',
         notify  => Service['network']
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
             content => "# Use Vac's script to check status\ncommand[check-vacd]=/var/lib/vac/bin/check-vacd 600\n",
             require => Package['nrpe'],
             owner   => 'root',
             group   => 'root',
             mode    => '0644',
             notify  => Service['nrpe'],
           }
    }

  #
  # Run APEL ssmsend
  #
  if ($apel_cert_path != '') and ($apel_key_path != '')
    {
      package { 'apel-ssm':
                ensure  => 'installed',
              }

      file { '/etc/grid-security/vac-apel-cert.pem':
             ensure  => 'file',
             source  => "$apel_cert_path",
             owner   => 'root',
             group   => 'root',
             mode    => '0644',
           }

      file { '/etc/grid-security/vac-apel-key.pem':
             ensure  => 'file',
             source  => "$apel_key_path",
             owner   => 'root',
             group   => 'root',
             mode    => '0600',
           }

      file { '/etc/cron.d/vac-ssmsend-cron':
             ensure  => 'file',
             content => "RANDOM_DELAY=55\n0 * * * * root /usr/bin/ssmsend -c /etc/apel/vac-ssmsend-prod.cfg >>/var/log/vac-ssmsend-cron.log 2>&1\n",
             owner   => 'root',
             group   => 'root',
             mode    => '0644',
           }
    }
}
