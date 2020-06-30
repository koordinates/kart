#!/bin/bash
set -euo pipefail

HERE=$(dirname "$(readlink -f "$0")")
USAGE="Usage: $0 deb|rpm ARCHIVE [target-distribution...]"

if [ $# -lt 2 ]; then
	echo "$USAGE"
	exit 2
fi

TYPE=$1
if [ "$TYPE" != "deb" ] && [ "$TYPE" != "rpm" ]; then
	echo "$USAGE"
	exit 2
fi
shift
ARCHIVE=$1
shift

DEB_ALL=(
	ubuntu:bionic
	ubuntu:xenial
	ubuntu:eoan
	debian:jessie-slim
	debian:stretch-slim
	debian:buster-slim
	debian:bullseye-slim
)
RPM_ALL=(
    fedora:latest
    centos:7
    centos:8
)

if [ "$TYPE" = "deb" ]; then
	TARGETS=${DEB_ALL[*]}
	INSTALL="dpkg -i"
else
	TARGETS=${RPM_ALL[*]}
	INSTALL="rpm -i"
fi

if [ $# != 0 ]; then
	TARGETS=$*
fi

for DIST in ${TARGETS[*]}; do
	echo -e "\n$DIST..."
	docker run \
		--rm \
		-i \
		-v "$(realpath "${HERE}/../../"):/src/:ro" \
		-w /root/ \
		"$DIST" \
		bash -ex \
		2>&1 <<-EOF | (while read -r; do echo "  $REPLY"; done)
			ln -sf /src/platforms/linux/sqlite3 /usr/bin/

			$INSTALL /src/platforms/linux/dist/$ARCHIVE
			command -v sno

			sno --version
			/src/tests/scripts/distcheck.sh
			/src/tests/scripts/e2e-1.sh
		EOF
done
