#  Author: Aniruddha Gokhale
#  Created: Fall 2021
#
#  Purpose: demonstrate serialization of a user-defined data structure using
#  FlatBuffers
#
#  Here our topic comprises a sequence number, a timestamp, a name, and a
#  data buffer of several uint32 numbers (whose value is not relevant to us) 

# The different packages we need in this Python driver code
import os
import sys
import time  # needed for timing measurements and sleep

import argparse  # argument parser
import json  # json package

##################################
#                      Driver program
##################################

def driver (name, iters, vec_len):

    print ("Driver program: Name = {}, Num Iters = {}, Vector len = {}".format (name, iters, vec_len))

    # let us create a Python dictionary object that we want to serialize
    # We call it a topic so we can relate it to the topic data that we want
    # to send to Kafka
    topic = {"seqnum": -1,
                   "name": name,
                   "ts": 0,
                   "vec": [item for item in range (vec_len)]}

    # now demo the capability for desired number of iterations.
    for i in range (iters):

        # fill the seq num and timestamp
        topic ["seqnum"] = i
        topic["ts"] = time.time ()
        
        # get a serialized buffer with seq num and some data items
        # In our Kafka code, the serialization will be done by the producer
        print ("Iteration #{}".format (i))
        print ("Serialize")

        # Note that JSON will convert things into string. Moreover, it can
        # do this for all known types. For anything beyond this, one must supply
        # an encoder
        start_time = time.time ()
        buf = json.dumps (topic)
        end_time = time.time ()
        print ("Serialization took {} secs".format (end_time-start_time))

        # In our Kafka code, the deserialization will be done by the consumer
        # now deserialize and see if it is printing the right thing
        print ("deserialize the topic")
        start_time = time.time ()
        retrieved_topic = json.loads (buf)
        end_time = time.time ()
        print ("Deserialization took {} secs".format (end_time-start_time))

        print ("Deserialized obj = {}".format (retrieved_topic))
        
        # sleep a while before we send the next serialization so it is not
        # extremely fast
        time.sleep (0.050)  # 50 msec


##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-i", "--iters", type=int, default=10, help="Number of iterations to run")
    parser.add_argument ("-n", "--name", default="FlatBuffer Demo", help="Name to include in each message")
    parser.add_argument ("-v", "--veclen", type=int, default=20, help="Length of the vector field (contents are irrelevant)")
    # parse the args
    args = parser.parse_args ()

    return args
    
#------------------------------------------
# main function
def main ():
    """ Main program """

    print("Demo program for Flatbuffer serialization/deserialization")

    # first parse the command line args
    parsed_args = parseCmdLineArgs ()
    
   # start the driver code
    driver (parsed_args.name, parsed_args.iters, parsed_args.veclen)

#----------------------------------------------
if __name__ == '__main__':
    main ()
