#!/bin/python
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Created: May 2017
# Modified: Oct 2019 to work with Python3
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

import argparse   # for command line parsing

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-d", "--dim", type=int, default=1000, help="Dimension of the square matrix, default 1000, i.e., 1000x1000")
    
    # add optional arguments
    parser.add_argument ("-i", "--iters", type=int, default=20, help="Number of iterations, default 20")
    
    # add positional arguments in that order
    parser.add_argument ("results_file", help="output file to store results")

    # parse the args
    args = parser.parse_args ()

    return args

#########################################
#  Matrix inverse operation
#########################################
def matrix_inverse (dim, iters, results_file):

    # create a randomly generated matrix once and find its inverse multiple times
    # Hopefully, it is not a singular matrix
    matrix = np.random.random ((dim,dim))

    for i in range (iters):
        print ("Iterations number = {}".format(i+1))
        # start timer
        start_time = time.time ()
        # compute the inverse
        mat_inv = linalg.inv (matrix)
        # stop the timer
        end_time = time.time ()

        # log the response time in the file
        resp_time = end_time - start_time
        results_file.write (str(i) + ", " + str(resp_time) + "\n")
        print ("Iteration = {}: Response time = {}".format (i, resp_time))
        

######################
# main program
######################
def main ():
    "Run a matrix inverse operation for specified number of iterations"

    try:
        # parse the command line
        parsed_args = parseCmdLineArgs ()

        print ("Matrix dimensions: {}, iterations = {} and log file = {}".format(parsed_args.dim, parsed_args.iters, parsed_args.results_file)
)
        # open the log file for saving results
        results_file = open (parsed_args.results_file, "w")

        # invoke the solver
        matrix_inverse (parsed_args.dim, parsed_args.iters, results_file)

        results_file.close ()
        
    except:
        print ("Error: {}".format (sys.exc_info()[0]))
        raise
    


# entry polint
if __name__ == "__main__":
    main ()
    
