#
# This spec file can be used to make an RPM containing a CernVM 3 ISO image
# from http://cernvm.cern.ch/portal/downloads
#
# - You can build the RPM with something like  rpmbuild -ba cernvm3iso.spec
# - The %build section downloads the ISO file based on the major, minor and 
#   release numbers given in the first few lines below 
# - The image is placed in /usr/share/images
# - As the major and minor version numbers are included in the package name, 
#   you can install RPMs containing more than one version at the same time
# - These RPMs are particularly useful on Vac factory machines where the
#   ISO images are supplied by the machine's administrator
#
%define major 1
%define minor 16
%define release 3
Version: %{major}.%{minor}
Release: %{release}
Name: cernvm3iso_%{major}_%{minor}
BuildArch: noarch
Summary: Micro CernVM 3 ISO image
Group: System Environment/Daemons
URL: http://www.gridpp.ac.uk/vac/
Vendor: CERN
License: CERN 
Packager: Andrew McNab <Andrew.McNab@cern.ch>
BuildRequires: curl

%description
Micro CernVM 3 ISO image

%build
curl http://cernvm.cern.ch/releases/ucernvm-images.%{version}-%{release}.cernvm.x86_64/ucernvm-prod.%{version}-%{release}.cernvm.x86_64.iso \
  >ucernvm-prod.%{version}-%{release}.cernvm.x86_64.iso
if [ $? != 0 ] ; then
 echo 'Failed to get ISO image!'
 exit 1
fi

%install
mkdir -p $RPM_BUILD_ROOT/usr/share/images/
cp -f ucernvm-prod.%{version}-%{release}.cernvm.x86_64.iso \
      $RPM_BUILD_ROOT/usr/share/images/

%files
/usr/share/images/ucernvm-prod.%{version}-%{release}.cernvm.x86_64.iso 
