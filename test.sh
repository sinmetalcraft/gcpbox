#!/bin/bash -eux

cd `dirname $0`

PATH=$(pwd)/build-cmd:$PATH
export PATH

go test ./...