#!/usr/bin/python

#
# Vanderbilt University, Computer Science
# CS4287-5287: Principles of Cloud Computing
# Author: Aniruddha Gokhale
# Created: Nov 2016
# 
#  Purpose: The code here is used to demonstrate the homegrown wordcount
# MapReduce framework on a network topology created using Mininet SDN emulator
#
# The mininet part is based on examples from the mininet distribution. The MapReduce
# part has been modified from the earlier thread-based implementation to a more
# process-based implementation required for this sample code
#

import os              # OS level utilities
import sys
import argparse   # for command line parsing

from signal import SIGINT
from time import time

import subprocess

# These are all Mininet-specific
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.net import CLI
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
from mininet.util import pmonitor

# This is our topology class created specially for Mininet
from mr_topology import MR_Topo

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # @NOTE@: You might need to make appropriate changes
    #                          to this logic. Just make sure.

    # add optional arguments
    parser.add_argument ("-p", "--masterport", type=int, default=5556, help="Wordcount master port number, default 5556")
    parser.add_argument ("-r", "--racks", type=int, choices=[1, 2, 3], default=1, help="Number of racks, choices 1, 2 or 3")
    parser.add_argument ("-M", "--map", type=int, default=10, help="Number of Map jobs, default 10")
    parser.add_argument ("-R", "--reduce", type=int, default=3, help="Number of Reduce jobs, default 3")
    
    # add positional arguments in that order
    parser.add_argument ("datafile", help="Big data file")

    # parse the args
    args = parser.parse_args ()

    return args
    
##################################
# Save the IP addresses of each host in our network
##################################
def saveIPAddresses (hosts, file="ipaddr.txt"):
    # for each host in the list, print its IP address in a file
    # The idea is that this file is now provided to the Wordcount
    # master program so it can use it to find the IP addresses of the
    # Map and Reduce worker machines
    try:
        file = open ("ipaddr.txt", "w")
        for h in hosts:
            file.write (h.IP () + "\n")

        file.close ()
        
    except:
            print(("Unexpected error:.format{}".sys.exc_info()[0]))
            raise

##################################
# run the entire map reduce set up on the hosts
#
# NOTE: I cannot get this to work properly so we are
# not going to use this approach
##################################
def runMapReduceWordCount (hosts, args):
    try:
        popens = {}

        print (hosts)
        
        # Start the master
        print("Running cmd: python ./mr_wordcount.py -m ", str (args.map), " -r ", str (args.reduce), " -p ", str (args.masterport), " ", args.datafile, " on host: ", hosts[0].IP())
        # popens [ master ] = master.popen ('ping', '-c3', master.IP() )
        popens [ hosts[0] ] = hosts[0].popen ('python', './mr_wordcount.py', '-m', str(args.map), '-r', str(args.reduce), '-p', str(args.masterport), args.datafile, stdout=subprocess.PIPE )

        # next run the Map workers
        # do this for as many map jobs as there are. Note that the hosts are organized as follows:
        # 0th entry is master, followed by map hosts, followed by reduce hosts
        for i in range (args.map):
            # the positional arguments for map process are its ID, master's IP addr, master's port
            print("Running cmd: python ./mr_mapworker.py ", str (i), " ", hosts[0].IP (), " ", str (args.masterport), " on host: ", hosts[i+1].IP())
            popens[ hosts[i+1] ] = hosts[i+1].popen ('python', './mr_map.py', str (i), hosts[0].IP (), str (args.masterport), stdout=subprocess.PIPE )

        # next run the Reduce workers

        print("Length of popens = ", len(list(popens.values ())))
        print("values = ", list(popens.values()))
        info ( "Monitoring output for 10 seconds\n" )
        endTime = time () + 10
        #for h, line in pmonitor( popens, timeoutms=500 ):
        for host, line in pmonitor( popens ):
            if host:
                info( '<%s>: %s' % ( host.name, line ) )

            #if time() >= endTime:
                #for p in popens.values():
                    #p.send_signal( SIGINT )
                    #outval = p.communicate ()[0]
                    #print "stdout = ", repr (outval)

    except:
            print("Unexpected error in run mininet:", sys.exc_info()[0])
            raise

##################################
#  Generate the commands file to be sources
#
# @NOTE@: You will need to make appropriate changes
#                          to this logic.
##################################
def genCommandsFile (hosts, args):
    try:
        # first remove any existing out files
        for i in range (len (hosts)):
            # check if the output file exists
            if (os.path.isfile (hosts[i].name+".out")):
                os.remove (hosts[i].name+".out")

        # create the commands file. It will overwrite any previous file with the
        # same name.
        cmds = open ("commands.txt", "w")

        # @NOTE@: You might need to make appropriate changes
        #                          to this logic by using the right file name and
        #                          arguments. My thinking is that the map and
        #                          reduce workers can be run as shown unless
        #                          you modify the command line params.

        # @NOTE@: for now I have commented the following line so we will have to
        # start the master manually on host h1s1

        # first create the command for the master
        #cmd_str = hosts[0].name + " python3 mr_wordcount.py -p " + str (args.masterport) + " -m " + str (args.map) + " -r " + str (args.reduce) + " " + args.datafile + " &> " + hosts[0].name + ".out &\n"
        #cmds.write (cmd_str)

        #  next create the command for the map workers
        for i in range (args.map):
            cmd_str = hosts[i+1].name + " python3 mr_mapworker.py " + hosts[0].IP () + " " + str (args.masterport) + " &> " + hosts[i+1].name + ".out &\n"
            cmds.write (cmd_str)

        #  next create the command for the reduce workers
        k = 1 + args.map   # starting index for reducer hosts (master + maps)
        for i in range (args.reduce):
            cmd_str = hosts[k+i].name + " python3 mr_reduceworker.py " + hosts[0].IP () + " " + str (args.masterport) + " &> " + hosts[k+i].name + ".out &\n"
            cmds.write (cmd_str)

        # close the commands file.
        cmds.close ()
        
    except:
            print("Unexpected error in run mininet:", sys.exc_info()[0])
            raise

######################
# main program
######################
def main ():
    "Create and run the Wordcount mapreduce program in Mininet topology"

    # parse the command line
    parsed_args = parseCmdLineArgs ()
    
    # instantiate our topology
    print("Instantiate topology")
    topo = MR_Topo (Racks=parsed_args.racks, M = parsed_args.map, R = parsed_args.reduce)

    # create the network
    print("Instantiate network")
    net = Mininet (topo, link=TCLink)

    # activate the network
    print("Activate network")
    net.start ()

    # debugging purposes
    print("Dumping host connections")
    dumpNodeConnections (net.hosts)

    # For large networks, this takes too much time so we
    # are skipping this. But it works.
    #
    # debugging purposes
    #print "Testing network connectivity"
    #net.pingAll ()
    
    #print "Running wordcount apparatus"
    # Unfortunately, I cannot get this to work :-(
    #runMapReduceWordCount (net.hosts, parsed_args)

    print("Generating commands file to be sourced")
    genCommandsFile (net.hosts, parsed_args)

    # run the cli
    CLI (net)

    # @NOTE@
    # You should run the generated commands by going to the
    # Mininet prompt on the CLI and typing:
    #     source commands.txt
    # Then, keep checking if all python jobs (except one) are completed
    # You can look at the *.out files which have all the debugging data
    # If there are errors in running the python code, these will also
    # show up in the *.out files.
    
    # cleanup
    net.stop ()

if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    main ()
