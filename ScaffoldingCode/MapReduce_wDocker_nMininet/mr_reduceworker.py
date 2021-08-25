#!/usr/bin/python
#
# Purpose:
# This code runs the wordcount reduce task. It runs inside the worker process.
# Since the worker gets commands from a master and sends result back to the
# master, we use ZeroMQ as a way to get this communication part done using
# the PUSH-PULL pattern
#
# Vanderbilt University, Computer Science
# CS4287-5287: Principles of Cloud Computing
# Author: Aniruddha Gokhale
# Created: Nov 2016
# Modified: Nov 2017 so it works inside a Docker container
# and unlike the mininet approach, does not exploit the fact that
# filesystem on all hosts is the same.
# 
#

import os
import sys
import time
import re
import zmq
import pickle

import argparse   # argument parser

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# @NOTE@: You will need to make appropriate changes
# to this logic.  You can maintain the overall structure
# but the logic of the reduce function has to change to
# suit the needs of the Assignment
#
# I do not think you need to change the class variables
# but you may need additional ones. The key change
# will be in do_work
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

# ------------------------------------------------
# Main reduce worker class        
class MR_Reduce ():
    """ The reduce worker class """
    def __init__ (self, args):
        """ constructor """
        self.master_ip = args.masterip
        self.master_port = args.masterport
        self.receiver = None  # connection to master
        self.init_sender = None     # for indicating worker up
        self.results_sender = None  # for sending map results

    #------------------------------------------
    def init_worker (self):
        """ Word count reduce worker initialization """
        print("initializing reduce worker")

        context = zmq.Context()

        # Socket to receive messages on. Worker uses PULL from the master
        # Note that the reducer uses 1 more than the base port of master
        self.receiver = context.socket (zmq.PULL)
        self.receiver.setsockopt (zmq.RCVHWM, 0)
        connect_addr = "tcp://"+ self.master_ip + ":" + str (self.master_port+1)

        print("Using PULL, reduce worker connecting to ", connect_addr)
        self.receiver.connect (connect_addr)
        
        # As part of the initialization, we tell the master that we are up.
        # This information is to be pushed to the master at a port which is
        # 2 more than the base of the master.
        self.init_sender = context.socket (zmq.PUSH)
        self.init_sender.setsockopt (zmq.LINGER, -1)
        connect_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+2)

        print("Using PUSH, reduce worker connecting to worker up barrier at ", connect_addr)
        self.init_sender.connect (connect_addr)
        #bind_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+2)
        #print "Using PUSH, reduce worker binding to worker up barrier at ", bind_addr
        #self.init_sender.bind (bind_addr)

        # now send an ACK to the barrier to let it know that we are up
        self.init_sender.send (b'0')

        # close the socket
        # self.init_sender.close ()

        # To send the results, we need to initialize the send address to point
        # to the reduce results barrier
        #
        # Note that the port number of the reduce result barrier is 4 more than
        # the port of the master. Initialize it so we can send results 
        self.results_sender = context.socket (zmq.PUSH)
        self.results_sender.setsockopt (zmq.LINGER, -1)
        self.results_sender.setsockopt (zmq.SNDHWM, 0)
        connect_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+4)

        print("Using PUSH, reduce worker connecting to map results barrier at ", connect_addr)
        self.results_sender.connect (connect_addr)
        #bind_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+4)
        #print "Using PUSH, reduce worker binding to results results barrier at ", bind_addr
        #self.results_sender.bind (bind_addr)
        

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes will be needed here for the Assignment
    #
    # The reduce logic executed here is tied to word count. This will need
    # modifications for the energy data
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    #------------------------------------------
    def do_work (self):
        """ Word count reduce function """
        print("starting work: reduce worker, working directory = ", os.getcwd())

        # receive the contents (which is received as a json)
        content = self.receiver.recv_json ()

        # final results for this worker are stored in this
        key_val_list = []
        
        # our contents will be a list of list. Each internal list could have
        # one or more entries for a given unique word
        for items in content:
            sum = 0
            for i in range(len(items)):
                sum = sum + items[i][1]

            # The [0]'th entry of each of the entries of the second level
            # list is the unique word. We just use the first one and dump it
            # into our list
            key_val_list.append ({'token': items[0][0], 'val': sum}) 


        # trigger the reduce barrier by sending the results back
        self.results_sender.send_json(key_val_list)

        # close the socket
        # self.results_sender.close ()

        time.sleep (5)
        print("reduce worker has completed its work")

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add positional arguments in that order
    # parser.add_argument ("id", type=int, help="worker number")
    parser.add_argument ("masterip", help="IP addr of master")
    parser.add_argument ("masterport", type=int, help="Port number of master")

    # parse the args
    args = parser.parse_args ()

    return args
    
#-----------------------------------------------------------------------
# main function
def main ():
    """ Main program for Reduce worker """

    print("MapReduce Reduce Worker program")
    parsed_args = parseCmdLineArgs ()
    
    # invoke the reducer logic
    reduceobj = MR_Reduce (parsed_args)

    # initialize the reduce worker network connections
    reduceobj.init_worker ()
    
    # invoke the reduce process. We run this reduce process forever
    while True:
        reduceobj.do_work ()
        print("MapReduce Reduce Worker done for this iteration")
        time.sleep (5)

#----------------------------------------------
if __name__ == '__main__':
    main ()
