#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013. All rights reserved.
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

include VERSION

INSTALL_FILES=vacd vac VAC.py vac-shutdown-vm vacd.init \
          make-vac-virtualmachines-conf check-vacd VERSION \
          vacd.8 vac.conf.5 check-vacd.8 vac.1 CHANGES \
          example.vac.conf
          
TGZ_FILES=$(INSTALL_FILES) Makefile vac.spec

vac.tgz: $(TGZ_FILES)
	mkdir -p TEMPDIR/vac
	cp $(TGZ_FILES) TEMPDIR/vac
	cd TEMPDIR ; tar zcvf ../vac.tgz vac
	rm -R TEMPDIR

install: $(INSTALL_FILES)
	mkdir -p $(RPM_BUILD_ROOT)/var/lib/vac/bin \
	         $(RPM_BUILD_ROOT)/var/lib/vac/etc \
	         $(RPM_BUILD_ROOT)/var/lib/vac/doc \
	         $(RPM_BUILD_ROOT)/var/lib/vac/tmp \
	         $(RPM_BUILD_ROOT)/var/lib/vac/images \
	         $(RPM_BUILD_ROOT)/var/lib/vac/vmtypes/example \
	         $(RPM_BUILD_ROOT)/var/lib/vac/machines
	cp vacd vac VAC.py vac-shutdown-vm check-vacd \
	   make-vac-virtualmachines-conf \
	   $(RPM_BUILD_ROOT)/var/lib/vac/bin
	cp VERSION vac.conf.5 vacd.8 CHANGES \
	   check-vacd.8 vac.1 example.vac.conf \
	   $(RPM_BUILD_ROOT)/var/lib/vac/doc
	cp example.README \
           $(RPM_BUILD_ROOT)/var/lib/vac/vmtypes/example/README
	mkdir -p $(RPM_BUILD_ROOT)/etc/rc.d/init.d
	cp vacd.init \
	   $(RPM_BUILD_ROOT)/etc/rc.d/init.d/vacd
	
rpm: vac.tgz
	export VAC_VERSION=$(VERSION) ; rpmbuild -ta vac.tgz
