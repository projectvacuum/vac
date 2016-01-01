Name: vac
Version: %(echo ${VAC_VERSION:-0.0})
Release: 1
BuildArch: noarch
Summary: Vac daemon and tools
License: BSD
Group: System Environment/Daemons
Source: vac.tgz
URL: https://www.gridpp.ac.uk/vac/
Vendor: GridPP
Packager: Andrew McNab <Andrew.McNab@cern.ch>
Requires: libvirt,libvirt-python,libvirt-client,qemu-kvm,genisoimage,bridge-utils,lvm2,dnsmasq >= 2.48-13,iptables,python-pycurl,m2crypto,openssl

%description
Vac implements the Vacuum model for running virtual machines. The vac RPM includes vacd daemon and vac command line tool.

%package -n vac-command
Group: Applications/Internet
Summary: Vac command (no daemon)

%description -n vac-command 
The vac-command RPM provides the vac command line tool but not the vacd daemon.

%prep

%setup -n vac

%build

%install
make install PYTHON_SITEARCH=%{python_sitearch} 

%preun
if [ "$1" = "0" ] ; then
  # if uninstallation rather than upgrade then stop
  service vacd stop

  # cleanup cron script too
  rm -f /etc/cron.d/vac-ssmsend
fi

%post
service vacd status
if [ $? = 0 ] ; then
  # if already running then restart with new version
  service vacd restart
fi

# Try to run ssmsend hourly, and make APEL-SYNC records at midday
echo `shuf -i 0-59 -n 1`' * * * * root /usr/bin/ssmsend -c /etc/apel/vac-ssmsend-prod.cfg >>/var/log/vac-ssmsend 2>&1' > /etc/cron.d/vac-ssmsend
echo '0 12 * * * root /usr/sbin/vac apel-sync >>/var/log/vac-ssmsend 2>&1' >>/etc/cron.d/vac-ssmsend

%files
/usr/sbin/*
/usr/share/man/man1/*
/usr/share/man/man5/*
/usr/share/man/man8/*
/usr/share/doc/vac-%{version}
%{python_sitearch}/vac
/var/lib/vac
/etc/rc.d/init.d/*
/etc/logrotate.d/vacd
/etc/vac.d
/etc/apel/vac-ssmsend-prod.cfg

%files -n vac-command
/usr/sbin/vac
/usr/share/man/man1/vac.1.gz
/usr/share/man/man5/vac.conf.5.gz
/usr/share/doc/vac-%{version}/CHANGES
/usr/share/doc/vac-%{version}/RELEASE
/usr/share/doc/vac-%{version}/VERSION
/usr/share/doc/vac-%{version}/vac.1
/usr/share/doc/vac-%{version}/vac.conf.5
/var/lib/vac/VERSION
%{python_sitearch}/vac
/etc/vac.d
