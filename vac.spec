Name: vac
Version: %(echo ${VAC_VERSION:-0.0})
Release: 1
BuildArch: noarch
Summary: Vac daemon and tools
License: BSD
Group: System Environment/Daemons
Source: vac.tgz
URL: http://www.gridpp.ac.uk/vac/
Vendor: GridPP
Packager: Andrew McNab <Andrew.McNab@cern.ch>
Requires: libvirt,libvirt-python,libvirt-client,qemu-kvm,genisoimage,nfs-utils,bridge-utils,lvm2,dnsmasq >= 2.48-13,iptables,python-pycurl

%description
Vac implements the Vacuum model for running virtual machines.

%prep

%setup -n vac

%build

%install
make install
mkdir -p $RPM_BUILD_ROOT/usr/sbin

# we are rpm so we can put files in /usr/sbin etc too
cp -f $RPM_BUILD_ROOT/var/lib/vac/bin/vac $RPM_BUILD_ROOT/usr/sbin

mkdir -p $RPM_BUILD_ROOT/usr/share/man/man1 $RPM_BUILD_ROOT/usr/share/man/man5 $RPM_BUILD_ROOT/usr/share/man/man8
cp -f $RPM_BUILD_ROOT/var/lib/vac/doc/vac.1 $RPM_BUILD_ROOT/usr/share/man/man1
cp -f $RPM_BUILD_ROOT/var/lib/vac/doc/vac.conf.5 $RPM_BUILD_ROOT/usr/share/man/man5
cp -f $RPM_BUILD_ROOT/var/lib/vac/doc/vacd.8 \
      $RPM_BUILD_ROOT/var/lib/vac/doc/check-vacd.8 \
           $RPM_BUILD_ROOT/usr/share/man/man8

%preun
if [ "$1" = "0" ] ; then
  # if uninstallation rather than upgrade then stop
  service vacd stop
fi

%post
service vacd status
if [ $? = 0 ] ; then
  # if already running then restart with new version
  service vacd restart
fi

%files
/usr/sbin/vac
/usr/share/man/man1
/usr/share/man/man5
/usr/share/man/man8
/var/lib/vac/bin
/var/lib/vac/etc
/var/lib/vac/doc
/var/lib/vac/tmp
/var/lib/vac/imagecache
/var/lib/vac/vmtypes
/var/lib/vac/machines
/var/lib/vac/apel-archive
/var/lib/vac/apel-outgoing
/var/lib/vac/machineoutputs
/etc/rc.d/init.d/vacd
/etc/logrotate.d/vacd
