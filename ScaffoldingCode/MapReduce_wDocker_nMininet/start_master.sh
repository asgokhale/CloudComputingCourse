#!/bin/sh
#
# Author: Aniruddha Gokhale
# Vanderbilt University
# EECS 4287-5287 Cloud Computing
# Created: Nov 2017
#
# Purpose: Script to start the master.
# Run this on the swarm master. In my case, the swarm master is my
# VM asg-ubuntu-vm on Horizon cloud.
#
# We maintain only one replica and constrain it to execute on the swarm
# master. Moreover, since we want to observe the results, we do not start it
# automatically but instead we do an exec into the container and then start the
# program manually
#
# Due to problems getting the federated container to even ping the master
# container when the swarm is federated, we are now mapping host port to
# container port. The assumption is that we will run the master on 5556 as
# its base, and then four additional ports from the base are used by the
# master. All of these are mapped using the -p option.
#
# This assumes that we have already created a swarm-wide network
docker service create --replicas 1 --name MyMR_Master --constraint 'node.hostname == asg-ubuntu-vm' -t --network MyMR_Network -p 5556:5556 -p 5557:5557 -p 5558:5558 -p 5559:5559 -p 5560:5560 129.59.107.155:5000/vu_mr_master /bin/bash
