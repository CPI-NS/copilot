#!/bin/bash
#
# epoaxos start script
#
##################


# use .all to get a list of nodes
all_nodes_addrs=($(echo "@.all.host@" | tr ',' ' '))


# -1 for the master
N=$(expr @.all | length@ - 1)
if [ "@.me.host@" == "@.all[0].host@" ]; then
  ./lib/master -N $N -twoLeaders=false
else
  ./lib/server -maddr=@.me.maddr@ -mport=@.me.mport@ -addr=@.me.host@ -port=@.me.port@ -copilot=false -e=true -exec=true -dreply=true
fi
