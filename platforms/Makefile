SHELL = /bin/bash

ifeq ($(OS),Windows_NT)
	PLATFORM := Windows
else
	PLATFORM := $(shell uname -s)
endif

SRC := $(realpath ..)
VERSION := $(strip $(shell cat ../sno/VERSION))

pyinstaller := $(abspath ../venv/bin/pyinstaller)

export PATH := $(abspath ../venv/bin):$(PATH)

pyapp-Linux = linux/dist/sno/sno_cli
pyapp-Darwin = macos/dist/Sno.app
deb = linux/dist/sno_$(VERSION)-1_amd64.deb
rpm = linux/dist/sno-$(VERSION)-1.x86_64.rpm
docker-fpm = linux/.docker-sno-fpm
pkg = macos/dist/Sno-$(VERSION).pkg

$(pyinstaller):
	$(MAKE) -C .. requirements/.tools.installed

.PHONY: all
all: $(deb) $(rpm)

ifeq ($(PLATFORM),Darwin)
all: $(pkg)
endif

.PHONY: clean
clean:
	-$(RM) -r linux/dist linux/build linux/.cache $(docker-fpm)
	-$(RM) -r macos/dist macos/build

$(docker-fpm): linux/fpm.Dockerfile
	docker build -t sno-fpm - < linux/fpm.Dockerfile
	touch $@

.PHONY: docker-fpm
docker-fpm:
	-$(RM) $(docker-fpm)
	$(MAKE) $(docker-fpm)

$(pyapp-Linux):
	$(MAKE) pyapp-Linux

.PHONY: pyapp-Linux
pyapp-Linux: $(SRC)/vendor/dist/vendor-Linux.tar.gz
	mkdir -p linux/.cache
	docker run \
		--rm \
		-it \
		--tmpfs /tmp \
		-v "$(SRC):/src" \
		-w /src \
		-v "$(abspath linux/.cache):/root/.cache" \
		--entrypoint "/bin/bash" \
		cdrx/pyinstaller-linux \
		-i -c '/src/platforms/linux/pyinstaller.sh || bash'

$(pyapp-Darwin):
	$(MAKE) pyapp-Darwin

.PHONY: pyapp-Darwin
pyapp-Darwin: $(pyinstaller) $(SRC)/vendor/dist/vendor-Darwin.tar.gz
	-$(RM) -r $(SRC)/vendor/dist/env/
	$(MAKE) -C $(SRC) sno-app

	tar xzf $(SRC)/vendor/dist/vendor-Darwin.tar.gz -C $(SRC)/vendor/dist/ env

	cd $(SRC) && \
	pyinstaller \
		--clean -y \
		--workpath platforms/macos/build/ \
		--distpath platforms/macos/dist/ \
		sno.spec

	-$(RM) -r $(SRC)/vendor/dist/env/

# App Code Signing
# CODESIGN should start with "Developer ID Application: ..."
	if [ -n "$(CODESIGN)" ]; then \
		codesign --deep -s "$(CODESIGN)" --entitlements macos/entitlements.plist -o runtime macos/dist/Sno.app; \
		codesign --verify --verbose macos/dist/Sno.app; \
	fi

	macos/dist/Sno.app/Contents/MacOS/sno_cli --version

.PHONY: pyapp
pyapp:
	$(MAKE) pyapp-$(PLATFORM)

.PHONY: deb
deb: $(deb)

.PHONY: rpm
rpm: $(rpm)

$(deb): PKGTYPE=deb
$(deb): VERSION:=$(VERSION)-1
$(rpm): PKGTYPE=rpm

$(deb) $(rpm): $(pyapp-Linux) | $(docker-fpm)
	docker run \
		--rm \
		-v $(abspath linux):/src \
		sno-fpm \
		./fpm.sh $(PKGTYPE) $(VERSION)

macos/build/%.rtf: macos/%.md
	sed -E 's/%VERSION%/$(VERSION)/' $< | pandoc -s - -o $@

$(pkg): $(pyapp-Darwin) macos/build/intro.rtf macos/build/summary.rtf
	packagesbuild \
		--verbose \
		--package-version $(VERSION) \
		macos/Sno.pkgproj

# Installer Signing
# PKGSIGN should start with "Developer ID Installer: ..."
	if [ -n "$(PKGSIGN)" ]; then \
		productsign --sign "$(PKGSIGN)" macos/dist/Sno.pkg $@; \
		pkgutil --check-signature $@; \
	else \
		mv macos/dist/Sno.pkg $@; \
	fi

# Notarization
# https://developer.apple.com/documentation/xcode/notarizing_macos_software_before_distribution/customizing_the_notarization_workflow
# NOTARIZE_USER should be the Apple Developer account email
# NOTARIZE_PASSWORD should be either the password, or "@keychain:AC_PASSWORD" for a keychain stored credential - see the doc above

.PHONY: pkg-notarize
pkg-notarize: $(pkg)
	pkgutil --check-signature $(pkg)
	xcrun altool --notarize-app \
		--primary-bundle-id "com.koordinates.Sno.SnoCore" \
		--username "$(NOTARIZE_USER)" \
		--password "$(NOTARIZE_PASSWORD)" \
		--file $(pkg)

.PHONY: pkg-notarize-check
pkg-notarize-check:
	xcrun altool --notarization-history 0 -u "$(NOTARIZE_USER)" -p "$(NOTARIZE_PASSWORD)"

# NOTARIZE_REQ should be a RequestUUID from pkg-notarize / pkg-notarize-check output
.PHONY: pkg-notarize-info
pkg-notarize-info:
	xcrun altool --notarization-info $(NOTARIZE_REQ) -u "$(NOTARIZE_USER)" -p "$(NOTARIZE_PASSWORD)"

.PHONY: pkg-notarize-staple
pkg-notarize-staple:
	xcrun stapler staple -v $(pkg)

.PHONY: pkg
pkg: $(pkg)

# docker images don't have sqlite3 in them - download a precompiled binary
linux/sqlite3:
	wget https://github.com/boramalper/sqlite3-x64/releases/download/3240000--2018-08-05T15.32.14Z/sqlite3 -O $@
	chmod +x $@

.PHONY: test-deb-all
test-deb-all: $(deb) linux/sqlite3
	linux/test-archive.sh deb $(<F)

.PHONY: test-rpm-all
test-rpm-all: $(rpm) linux/sqlite3
	linux/test-archive.sh rpm $(<F)

.PHONY: test-bin-all
test-bin-all: $(pyapp-Linux)
	linux/test-bin.sh

.PHONY: test-all
test-all: test-bin-all test-deb-all test-rpm-all

