#!/bin/sh -eux

cd `dirname $0`

go mod download

# build tools
rm -rf build-cmd/
mkdir build-cmd

export GOBIN=`pwd -P`/build-cmd

go install github.com/sinmetalcraft/gcpbox/_cmd/myvet
go install honnef.co/go/tools/cmd/staticcheck