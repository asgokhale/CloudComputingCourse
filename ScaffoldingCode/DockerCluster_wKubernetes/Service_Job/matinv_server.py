#!/bin/python
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Created: May 2017
# Modified: Oct 2019 to work with Python3
# Modified: Spring 2021 to make it into a client-server program using ZMQ
#
# Purpose: To provide a docker container that can do matrix inversion
#
# import system headers
import sys

# import timer stuff to record the time
import time

# number crunching
import numpy as np
from numpy import linalg

# json
import json

# zmq
import zmq

#########################################
#  Matrix inverse operation
#########################################
def matrix_inverse (dims):

    # create a randomly generated matrix once and find its inverse multiple times
    # Hopefully, it is not a singular matrix
    matrix = np.random.random ((dims,dims))

    # start timer
    start_time = time.time ()

    # compute the inverse
    mat_inv = linalg.inv (matrix)

    # stop the timer
    end_time = time.time ()

    # log the response time in the file
    exec_time = end_time - start_time

    # return the value
    return exec_time

######################
# main program
######################
def main ():
    "Run a matrix inverse operation"

    try:
        # first obtain zmq context
        print ("Obtain zmq context")
        context = zmq.Context ()

        # now instantiate a server obj
        print ("Obtain the socket handle")
        server = context.socket (zmq.REP)
        server.bind ("tcp://*:5556")

        # forever loop
        print ("Start serving")
        while True:
            request = server.recv_json ()

            # compute the inverse
            exec_time = matrix_inverse (request["dims"])

            # construct a dict obj
            reply = {
                "exec": exec_time
            }

            # send reply
            server.send_json (reply)
            
        
    except:
        print ("Error: {}".format (sys.exc_info()[0]))
        raise
    

#-------------------------------------------------------------------------------
# entry polint
if __name__ == "__main__":
    main ()
    
