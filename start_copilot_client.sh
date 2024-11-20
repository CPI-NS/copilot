#!/bin/bash


#./lib/ClientDriverGo -maddr=node1 -mport=7087 -twoLeaders=true -id=0
set -x
./lib/ClientDriverGo -maddr=node1 -mport=7087 -twoLeaders=true -id=$CLIENT_ID
