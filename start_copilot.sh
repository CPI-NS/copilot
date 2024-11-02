#!/bin/bash
#
# Copilot start script
#
##################


# use .all to get a list of nodes
all_nodes_addrs = ($(echo "@.all_sut_nodes.host@" | tr ':' ' ')) 


# -1 for the master
N=$(expr ${#all_node_addrs[@]} - 1)
if [ .me.host == .all[0]; then
  ./bin/master -N $N -twoLeaders=true
else
  ./bin/server -maddr=@.me.maddr@ -mport=@.me.mport@ -addr=@.me.host@ -port=@.me.port@ -copilot=true -exec=true -dreply=true
fi
