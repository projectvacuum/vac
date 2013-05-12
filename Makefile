#!/bin/sh
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

INSTALL_FILES=vacd VAC.py vac-shutdown-vm vacd.init \
          make-vac-virtualmachines-conf
          
TGZ_FILES=$(INSTALL_FILES) Makefile vac.spec

vac.tgz: $(TGZ_FILES)
	mkdir -p TEMPDIR/vac
	cp $(TGZ_FILES) TEMPDIR/vac
	cd TEMPDIR ; tar zcvf ../vac.tgz vac
	rm -R TEMPDIR

install: $(INSTALL_FILES)
	mkdir -p /var/lib/vac/bin \
	         /var/lib/vac/etc \
	         /var/lib/vac/doc \
	         /var/lib/vac/tmp \
	         /var/lib/vac/images \
	         /var/lib/vac/vmtypes \
	         /var/lib/vac/machines
	cp vacd VAC.py vac-shutdown-vm vacd.init \
	   make-vac-virtualmachines-conf \
	   /var/lib/vac/bin

rpm: vac.tgz
	rpmbuild -ta vac.tgz
