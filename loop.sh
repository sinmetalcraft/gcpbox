#!/bin/sh

for i in `seq 0 1000`
do
  go test -v github.com/sinmetalcraft/gcpbox/cloudtasks -count=1
done