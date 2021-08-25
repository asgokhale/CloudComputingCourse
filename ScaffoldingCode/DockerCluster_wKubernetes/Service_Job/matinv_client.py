#!/bin/python
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Created: May 2017
# Modified: Oct 2019 to work with Python3
# Modified: Spring 2021 to make it into a client-server program using ZMQ
#
# Purpose: To provide a docker container that works as a client to another docker
# container that serves the client request.
#
# import system headers
import sys

# import timer stuff to record the time
import time

# json
import json

# zmq
import zmq

import argparse   # for command line parsing

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-a", "--addr", default="localhost:5556", help="IP address and port to reach the server, default localhost:5556")

    parser.add_argument ("-d", "--dim", type=int, default=1000, help="Dimension of the square matrix, default 1000, i.e., 1000x1000")
    
    # add optional arguments
    parser.add_argument ("-i", "--iters", type=int, default=20, help="Number of iterations, default 20")
    
    # add positional arguments in that order
    parser.add_argument ("results_file", help="output file to store results")

    # parse the args
    args = parser.parse_args ()

    return args

#########################################
#   send request
#########################################
def send_request (server, dims, iters, results_file):

    try:
        # we now create a json structure and send it to the server
        print ("Create json structure to send")
        request = {
            "dims": dims
        }

        # Here we make requests to the server for the specified number of
        # iterations
        print ("Send_request for {} number of iterations".format (iters))

        for i in range (iters):
            #send request
            print ("Sending request number {}".format (i))
            server.send_json (request)

            # start timer
            start_time = time.time ()
            
            # wait for reply
            # when using this code for measurements, comment this print stmt
            print ("waiting to receive reply")
            reply = server.recv_json ()

            # end timer
            end_time = time.time ()
            resp_time = end_time - start_time
           
            # insert result in file
            print ("Write results into the file")
            results_file.write (str(i) + ", " + str (reply["exec"]) + ", " + str (resp_time) + "\n")

            print ("Iteration = {}: Round trip time = {}".format (i, resp_time))

    except:
        print ("Error: {}".format (sys.exc_info()[0]))
        raise

######################
# main program
######################
def main ():
    "Run a matrix inverse client for specified number of iterations"

    try:
        # parse the command line
        print ("Parse the command line")
        parsed_args = parseCmdLineArgs ()

        print ("Server: {}; Matrix dimensions: {}, iterations: {} and log file: {}".format(parsed_args.addr, parsed_args.dim, parsed_args.iters, parsed_args.results_file)
)
        # get ZMQ context
        print ("Obtain ZMQ context")
        context = zmq.Context()

        # create a handle to the server
        print ("Connecting to matrix inverse server...")
        server = context.socket (zmq.REQ)
        server.connect ("tcp://"+parsed_args.addr)

        # open the log file for saving results
        print ("Open results file for writing")
        results_file = open (parsed_args.results_file, "w")

        # send the request to the server and wait for results
        print ("Making a request on the server")
        send_request (server, parsed_args.dim, parsed_args.iters, results_file)
        
        # close the file and we are done
        print ("Close the results file")
        results_file.close ()

        # close connection to server
        print ("Close connection to server")
        server.close ()
        
    except:
        print ("Error: {}".format (sys.exc_info()[0]))
        raise
    

#--------------------------------------------------------------------------------------
# entry polint
if __name__ == "__main__":
    main ()
    
