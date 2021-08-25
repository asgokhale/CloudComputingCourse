#!/usr/bin/python
#
# Purpose:
# This code runs the wordcount map task. It runs inside the worker process.
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

import os
import sys
import time
import re
import zmq
import json

import argparse   # argument parser

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# @NOTE@: You will need to make appropriate changes
# to this logic.  You can maintain the overall structure
# but the logic of the map function has to change to
# suit the needs of the Assignment
#
# I do not think you need to change the class variables
# but you may need additional ones. The key change
# will be in the function do_work
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

# ------------------------------------------------
# Main map worker class        
class MR_Map ():
    """ The map worker class """
    def __init__ (self, args):
        """ constructor """
        self.id = None   # we get an ID on the fly
        self.master_ip = args.masterip
        self.master_port = args.masterport
        self.receiver = None  # connection to master
        self.init_sender = None     # for indicating worker up
        self.results_sender = None  # for sending map results

    #------------------------------------------
    def init_worker (self):
        """ Word count map worker initialization """
        print("initializing map worker in directory: ", os.getcwd ())

        context = zmq.Context()

        # Socket to receive messages on. Worker uses PULL from the master
        # To that end, we connect to the server. The map worker pulls info
        # from the base port of the master
        self.receiver = context.socket (zmq.PULL)
        self.receiver.setsockopt (zmq.RCVHWM, 0)
        connect_addr = "tcp://"+ self.master_ip + ":" + str (self.master_port)
        print("Using PULL, map worker connecting to ", connect_addr)
        self.receiver.connect (connect_addr)
        
        # As part of the initialization, we tell the master that we are up.
        # This information is to be pushed to the master at a port which is
        # 2 more than the base of the master.
        self.init_sender = context.socket (zmq.PUSH)
        self.init_sender.setsockopt (zmq.LINGER, -1)
        connect_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+2)
        print("Using PUSH, map worker connecting to worker up barrier at ", connect_addr)
        self.init_sender.connect (connect_addr)
        #bind_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+2)
        #print "Using PUSH, map worker binding to worker up barrier at ", bind_addr
        #self.init_sender.bind (bind_addr)

        # now send an ACK to the barrier to let it know that we are up
        self.init_sender.send (b'0')

        # close the socket
        # self.init_sender.close ()

        # To send the results, we need to initialize the send address to point
        # to the map results barrier
        #
        # Note that the port number of the maps result barrier is 3 more than
        # the port of the master. Initialize it so we can send results 
        self.results_sender = context.socket (zmq.PUSH)
        self.results_sender.setsockopt (zmq.LINGER, -1)
        self.results_sender.setsockopt (zmq.SNDHWM, 0)
        connect_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+3)
        print("Using PUSH, map worker connecting to map results barrier at ", connect_addr)
        self.results_sender.connect (connect_addr)
        #bind_addr = "tcp://" + self.master_ip + ":" + str (self.master_port+3)
        #print "Using PUSH, map worker binding to map results barrier at ", bind_addr
        #self.results_sender.bind (bind_addr)
        


    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes will be needed here for the Assignment
    #
    # The map logic executed here is tied to word count. This will need
    # modifications for the energy data
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    #------------------------------------------
    def do_work (self):
        """ Word count map function """

        # recall that the master broadcasts the map or reduce message
        # via the PUSH.  Receive the information from the master and
        # process it
        json_obj = self.receiver.recv_json()
        print("map received json message")
        
        # now parse the json object and do the work
        self.id = json_obj['id']
        content = json_obj['content']
        
        print("do_work: map worker received id: ", self.id)

        # tokenize the incoming chunk, which is a string. We want the
        # list to be only words and nothing else. So rather than the simple
        # split method of the string class, we use regexp's split

        # We allow apostrophe. This is the pattern we are going to search for
        # in the incoming chunk and tokenize the stream accordingly. Each token
        # then is a word
        pattern = "(\s|~|'|\!|@|\#|\$|%|\^|&|\*|\(|\)|-|_|\+|=|\{|\}|\[|\]|\||\||:|;|\"|<|>|\,|\.|\?|\/)+"

        # tokenize the stream according to the pattern above
        split_arg = re.split (pattern, content)

        # create a reg expression pattern against which we are going
        # to match against. We allow a word with apostrophe. The following
        # regular expression gives us valid words (only alphabets but
        # apostrophe is ok but not at the beginning. So something like don't
        # will be captured as a unique word)
        pattern = re.compile ("([A-Za-z]+)('[A-Za-z])?")

        # intermediate keys and values are stored in this array
        intmed_key_val_list = []

        # For every element in the split, if it belongs to a sensical
        # word, emit it as an intermediate key with its count
        for token in split_arg:
            # now check if it is a valid word
            if pattern.match (token):
                # emit the intermediate key and its occurrence as a 1
                intmed_key_val_list.append ({'token': token, 'val': 1})

        # now we send the results of the map phase to the master
        # The message is a json msg
        self.results_sender.send_json (intmed_key_val_list)

        # close the socket
        # self.results_sender.close ()
        
        print("map worker with ID: ", self.id, " work is performed")

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add positional arguments in that order
    parser.add_argument ("masterip", help="IP addr of master")
    parser.add_argument ("masterport", type=int, help="Port number of master")

    # parse the args
    args = parser.parse_args ()

    return args
    
#--------------------------------------------------------------------
# main function
def main ():
    """ Main program for Map worker """

    print("MapReduce Map Worker program")

    # first parse the command line arguments
    parsed_args = parseCmdLineArgs ()
    
    # instantiate a map object with the parsed args
    mapobj = MR_Map (parsed_args)

    # initialize the map worker network connections
    mapobj.init_worker ()
    
    # invoke the map process. We run this map process forever
    while True:
        mapobj.do_work ()
        print("MapReduce Map Worker done for this iteration")
        time.sleep (5)
        
    
#----------------------------------------------
if __name__ == '__main__':
    main ()
