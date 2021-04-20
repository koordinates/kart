SHELL = /bin/bash

ifeq ($(OS),Windows_NT)
	$(error "On Windows, run `nmake /f makefile.vc` instead.")
else
	PLATFORM := $(shell uname -s)
endif

SRC := $(realpath ..)
VERSION := $(strip $(shell cat ../sno/VERSION))

pyinstaller := $(abspath ../venv/bin/pyinstaller)

export PATH := $(abspath ../venv/bin):$(PATH)

pyapp-Linux = linux/dist/kart/kart_cli
pyapp-Darwin = macos/dist/Kart.app
deb = linux/dist/kart_$(VERSION)-1_amd64.deb
rpm = linux/dist/kart-$(VERSION)-1.x86_64.rpm
docker-fpm = linux/.docker-kart-fpm
pkg = macos/dist/Kart-$(VERSION).pkg

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
	docker build -t kart-fpm - < linux/fpm.Dockerfile
	touch $@

.PHONY: docker-fpm
docker-fpm:
	-$(RM) $(docker-fpm)
	$(MAKE) $(docker-fpm)

$(pyinstaller):
	$(MAKE) -C .. py-tools

$(pyapp-Linux):
	$(MAKE) pyapp-Linux

.PHONY: pyapp-Linux
pyapp-Linux: $(SRC)/vendor/dist/vendor-Linux.tar.gz
	mkdir -p linux/.cache
	docker run \
		--rm \
		$(if $(MANYLINUX_DEBUG),-it) \
		--tmpfs /tmp \
		-v "$(SRC):/src" \
		-w /src \
		-v "$(abspath linux/.cache):/root/.cache" \
		--entrypoint "/bin/bash" \
		cdrx/pyinstaller-linux \
		-i -c '/src/platforms/linux/pyinstaller.sh $(if $(MANYLINUX_DEBUG),|| bash)'

$(pyapp-Darwin):
	$(MAKE) pyapp-Darwin

.PHONY: app-sign
app-sign:
# App Code Signing
# CODESIGN should start with "Developer ID Application: ..."
	codesign -vvv --deep --timestamp --strict -s "$(CODESIGN)" --entitlements macos/entitlements.plist -o runtime macos/dist/Kart.app
	codesign --verify --verbose macos/dist/Kart.app
#	spctl -vvv --assess --type exec macos/dist/Kart.app

.PHONY: pyapp-Darwin
pyapp-Darwin: $(pyinstaller) $(SRC)/vendor/dist/vendor-Darwin.tar.gz
	-$(RM) -r $(SRC)/vendor/dist/env/
	$(MAKE) -C $(SRC) release

	tar xzf $(SRC)/vendor/dist/vendor-Darwin.tar.gz -C $(SRC)/vendor/dist/ env

	cd $(SRC) && \
	pyinstaller \
		--clean -y \
		--workpath platforms/macos/build/ \
		--distpath platforms/macos/dist/ \
		kart.spec

	-$(RM) -r $(SRC)/vendor/dist/env/

ifdef CODESIGN
	$(MAKE) app-sign
endif

	macos/dist/Kart.app/Contents/MacOS/kart_cli --version

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
		kart-fpm \
		./fpm.sh $(PKGTYPE) $(VERSION)

macos/build/%.rtf: macos/%.md
	sed -E 's/%VERSION%/$(VERSION)/' $< | pandoc -s - -o $@

.PHONY: pkg-sign
pkg-sign:
	# Installer Signing
	# PKGSIGN should start with "Developer ID Installer: ..."
	productsign --sign "$(PKGSIGN)" macos/dist/Kart.pkg $(pkg)
	pkgutil --check-signature $(pkg)

$(pkg): $(pyapp-Darwin) macos/build/intro.rtf macos/build/summary.rtf
	packagesbuild \
		--verbose \
		--package-version $(VERSION) \
		macos/Kart.pkgproj

ifdef PKGSIGN
	$(MAKE) pkg-sign
else
	mv macos/dist/Kart.pkg $@
endif

# Notarization
# https://developer.apple.com/documentation/xcode/notarizing_macos_software_before_distribution/customizing_the_notarization_workflow
# NOTARIZE_USER should be the Apple Developer account email
# NOTARIZE_PASSWORD should be either the password; or "@keychain:AC_PASSWORD" for a keychain stored credential;
#   or "@env:ENV_VAR_NAME" - see the doc above

.PHONY: pkg-notarize
pkg-notarize: $(pkg)
	pkgutil --check-signature $(pkg)
	xcrun altool --notarize-app \
		--primary-bundle-id "com.koordinates.Kart.KartCore" \
		--username "$(NOTARIZE_USER)" \
		--password "$(NOTARIZE_PASSWORD)" \
		--file $(pkg) \
		--output-format xml \
		| tee macos/build/notarize-app-result.plist
	echo Notarization-RequestUUID=`$(PLIST_BUDDY) -c "Print notarization-upload:RequestUUID" macos/build/notarize-app-result.plist`

.PHONY: pkg-notarize-check
pkg-notarize-check:
	xcrun altool --notarization-history 0 -u "$(NOTARIZE_USER)" -p "$(NOTARIZE_PASSWORD)"

# NOTARIZE_REQ should be a RequestUUID from pkg-notarize / pkg-notarize-check output
.PHONY: pkg-notarize-info
pkg-notarize-info:
	xcrun altool --notarization-info \
		"$(NOTARIZE_REQ)" \
		-u "$(NOTARIZE_USER)" \
		-p "$(NOTARIZE_PASSWORD)" \
		--output-format xml \
		| tee macos/build/notarize-info-result.plist

.PHONY: pkg-notarize-staple
pkg-notarize-staple:
	xcrun stapler staple -v $(pkg)
	spctl -a -vvv -t install $(pkg)
	stapler validate $(pkg)

# Full Notarization Workflow
# Inspired by https://github.com/adib/DiskImageDistribution/blob/master/BuildScripts/make_disk_image.sh
.PHONY: ci-pkg-notarize
ci-pkg-notarize:
	$(MAKE) pkg-notarize

	export NOTARIZE_REQ=`/usr/libexec/PlistBuddy -c "Print notarization-upload:RequestUUID" macos/build/notarize-app-result.plist`; \
	echo "Checking notarization status: $$NOTARIZE_REQ ..."; \
	for (( ; ; )); do \
		$(MAKE) pkg-notarize-info; \
		NOTARIZE_STATUS=`/usr/libexec/PlistBuddy -c "Print notarization-info:Status" macos/build/notarize-info-result.plist`; \
		if [ "$$NOTARIZE_STATUS" = "success" ]; then \
			echo "Notarization succeeded"; \
			break; \
		elif [ "$$NOTARIZE_STATUS" != "in progress" ]; then \
			echo "Notarization failed: $$NOTARIZE_STATUS"; \
			exit 1; \
		fi; \
		sleep 10; \
	done

	echo "Stapling notarization result..."; \
	for (( ; ; )); do \
		xcrun stapler staple -v $(pkg) || R=$?; \
		if [[ $$R -eq 0 ]]; then \
			echo "Stapling succeeded"; \
			break; \
		elif [[ $$R -ne 65 ]]; then \
			echo "Stapling failed: $$R"; \
			exit 1; \
		fi; \
        echo "Waiting for stapling to find record..."; \
		sleep 10; \
	done

	@echo "Validating $(pkg) ..."
	spctl -a -vvv -t install $(pkg)
	stapler validate $(pkg)


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
