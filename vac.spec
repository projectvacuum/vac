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
Requires: libvirt,libvirt-python,libvirt-client,qemu-kvm,genisoimage,nfs-utils,bridge-utils,lvm2,dnsmasq >= 2.48-13,iptables,python-pycurl,m2crypto

%description
Vac implements the Vacuum model for running virtual machines.

%prep

%setup -n vac

%build

%install
make install PYTHON_SITEARCH=%{python_sitearch} 

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
/usr/sbin/*
/usr/share/man/man1/*
/usr/share/man/man5/*
/usr/share/man/man8/*
/usr/share/doc/vac-%{version}
%{python_sitearch}/vac
/var/lib/vac
/etc/rc.d/init.d/vacd
/etc/logrotate.d/vacd
/etc/vac.d
/etc/apel/vac-ssmsend-prod.cfg
