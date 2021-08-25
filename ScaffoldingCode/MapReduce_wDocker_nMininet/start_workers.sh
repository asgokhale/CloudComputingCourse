#!/bin/sh
#
# Author: Aniruddha Gokhale
# Vanderbilt University
# EECS 4287-5287 Cloud Computing
# Created: Nov 2017
#
# Purpose: Script to start the map and reduce workers
#
# Depending on your need, change the replica numbers accordingly
# Here we disallow spawning any of our workers on the master node. Swarm
# will load balance them appropriately on other nodes of your swarm
#
# Notice also that we supply the floating IP of the master host which will then
# map it to the right container port.
#
# Change the floating IP to yours. Change host names to yours.
#
# The restart condition of "none" prevents the manager from restarting the
# workers after they are done.
docker service create --restart-condition "none" --replicas 10 --name MyMR_Map --constraint 'node.hostname != asg-ubuntu-vm' -t --network MyMR_Network 129.59.107.155:5000/vu_mr_map python /root/mr_mapworker.py 129.59.107.155 5556
docker service create --restart-condition "none" --replicas 3 --name MyMR_Reduce --constraint 'node.hostname != asg-ubuntu-vm' -t --network MyMR_Network 129.59.107.155:5000/vu_mr_reduce python /root/mr_reduceworker.py 129.59.107.155 5556
