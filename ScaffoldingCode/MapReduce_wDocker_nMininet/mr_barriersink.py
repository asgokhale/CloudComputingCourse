#!/usr/bin/python
#  
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Course: CS4287-5287 Principles of Cloud Computing
# Created: Fall 2016
#
# Purpose: Barrier sync
#

# system and time
import os
import sys
import time
import argparse   # argument parser
import zmq               # ZeroMQ library

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-p", "--port", type=int, default=5558, help="Barrier  port, default 5558")
    
    # add positional arguments in that order
    parser.add_argument ("cond", type=int, help="Barrier Condition")

    # parse the args
    args = parser.parse_args ()

    return args
    
#------------------------------------------
# barrier thread for map and reduce phases
def barrier_sink (args):
    "map or reduce phase barrier"

    try:
        # here we start the ZeroMQ sink task. When the required number of
        # map or reduce responses are received, the thread terminates.
        print("Barrier sink task starting with args: ", args)

        context = zmq.Context ().instance ()

        # Socket to receive messages on. Note that the sink uses the PULL pattern
        receiver = context.socket (zmq.PULL)
        bind_addr = "tcp://*:" + str (args.port) 
        receiver.bind (bind_addr)   # use the port from the arg
        print("Using PULL, barrier is binding to ", bind_addr)

        # Wait for kickstart msg from the master
        s = receiver.recv ()
        print("Received kickstart message; now waiting to receive messages from workers")

        i = 0
        while True:
            # wait until all responses received from workers
            s = receiver.recv ()
            print("Barrier received an ACK")
            i = i + 1
            if ( i == args.cond):  # we have reached the required number of acks.
                print("Barrier received required number of ACKS = ", args.cond)
                break
            
    except:
        print("Unexpected error in barrier process:", sys.exc_info()[0])


# main function
def main ():
    """ Main program """

    print("MapReduce Wordcount Barrier Sync program")
    parsed_args = parseCmdLineArgs ()

    barrier_sink (parsed_args)

#----------------------------------------------
if __name__ == '__main__':
    main ()
        
