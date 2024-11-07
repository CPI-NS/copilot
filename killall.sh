#!/bin/bash

for pid in $(ps x | grep "master\|server" | grep -v "grep" | awk '{ print $1 }'); do
    kill $pid
done
