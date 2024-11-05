#!/bin/bash

for pid in $(ps x | grep "master|server" | awk '{ print $1 }'); do
    kill $pid
done
