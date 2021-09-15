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

import serialize as sz  # this is the from the file serialize.py in the same directory

##################################
#                      Driver program
##################################

def driver (name, iters, vec_len):

    print ("Driver program: Name = {}, Num Iters = {}, Vector len = {}".format (name, iters, vec_len))
    
    # now publish our information for the number of desired iterations
    for i in range (iters):

        # get a serialized buffer with seq num and some data items
        print ("Iteration #{}".format (i))
        print ("serialize the topic")

        # here we are calling our serialize method passing it
        # the iteration number, the topic identifier, and length.
        # The underlying method creates some dummy data, fills
        # up the data structure and serializes it into the buffer
        start_time = time.time ()
        buf = sz.serialize (i, name, vec_len)
        end_time = time.time ()
        print ("Serialization took {} secs".format (end_time-start_time))

        # now deserialize and see if it is printing the right thing
        print ("deserialize the topic")
        start_time = time.time ()
        sz.deserialize (buf)
        end_time = time.time ()
        print ("Deserialization took {} secs".format (end_time-start_time))
        
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
