#!/usr/bin/python
#  
# Purpose: MapReduce Framework for Wordcount (not yet a generic framework)
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Course: CS4287-5287 Principles of Cloud Computing
# Created: Fall 2016
# Modified: Nov 2017 to make it work in Docker containers
#
# It can still be used with Mininet; in fact now it actually
# sends data over the network and will thus showcase real performance
# results 
#
#

# system and time
import os
import sys
import time

import re                    # regular expression
import csv                   # deal with CSV files
import operator              # used in itertools
import itertools             # nice iterators

import zmq                   # ZeroMQ library
import json                  # json
import pickle                # serialization

import subprocess as sp      # unused in this impl

from mr_thread import MR_Thread  # our Map Reduce threading class

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# @NOTE@: changes will be needed here for Assignment
#
# The structure of the barrier sink will remain same. However,
# since the intermediate key and val may be different, you
# might need to make change to how the files are saved
#
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
#--------------------------------------------------------------------------
# barrier function for map and reduce phases. It is used in two scenarios.
# (1) as a barrier to ensure that all map and reduce workers are up
# (2) as a barrier to ensure that results from map and reduce workers arrive
#
# This function is executed by a thread in parallel to the main program, where
# the main program will block until all desired responses are received.
#
def barrier_sink (args):
    "map or reduce phase barrier"

    try:
        # The master serves as a sink to receive the appropriate number of
        # responses from workers under two conditions: to make sure they
        # are up and running, and to receive results from the map and
        # reduce phases.
        #
        # To do that, we start the ZeroMQ sink task. When the required number
        # of map or reduce responses are received, the thread terminates
        # doing any processing that is needed (e.g., saving the data in
        # a file for results from map and reduce phases).
        
        print("Barrier sink thread starting with args: ", args)
        receiver = args['receiver']

        # The logic here is that we wait until the required number of
        # ACKs are received 
        i = 0
        while (i < args['cond']):
            # wait until all responses received from workers. For different
            # operations we have a different behavior
            if (args['op'] == "workers_up"):
                # when the map and reduce worker are initialized the first
                # time, they just send a single byte to let us know they
                # are up. We just receive that byte and ignore it but make
                # a note of the count
                s = receiver.recv ()
                
            elif (args['op'] == "map_results"):
                # when a map worker completes its job, it is going to send
                # us the stream of intermediate keys and the value. We are
                # going to save it in a local csv file for use in our shuffle
                # phase.  The csv approach is done because the original code
                # had assumed that the map results are in a csv file.
                #
                # We could possibly be doing this in a simpler way but for
                # now it is done this way.
                #
                # Note that our map worker sends us json-ified response. So
                # receive a json response.
                map_resp = receiver.recv_json ()

                # save this into a csv file. Number it based on the iteration
                # we are in.
                map_file = open ("Map"+str(i)+".csv", "w")

                # write all the entries into the csv file
                for j in range (len(map_resp)):
                    map_file.write (map_resp[j]['token'] + str (",") + str(map_resp[j]['val']) + "\n")
                # close the file
                map_file.close ()
                
            elif (args['op'] == "reduce_results"):
                # results from the reduce phase are also in json format
                reduce_resp = receiver.recv_json ()

                # Each reduce task saves its results in a file
                reduce_file = open ("Reduce"+str(i)+".csv", "wt")

                # write the csv entries to the file.
                for j in range (len(reduce_resp)):
                    reduce_file.write (reduce_resp[j]['token'] + str (",") + str(reduce_resp[j]['val']) + "\n")
                    
                # close the file
                reduce_file.close ()

            else:
                print("Unknown operation. Should be aborted")

            # increment the number of acks received
            i = i + 1
            # while this is good for debugging, I think this
            # eats up time.
            # print ("Barrier Ack# {} received".format(i))

        print("Barrier received required number of ACKS = ", args['cond'])
        return
    
    except:
        print("Unexpected error in barrier thread: ", sys.exc_info()[0])

# ------------------------------------------------
# The master for map-reduce. We capture its features in a class 
#
class MR_Framework ():
    """ The map reduce orchestrator class """

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes will be needed here for the Assignment
    #
    # Add any additional class members you may need or remove any that
    # you do not need.
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    #################################################################
    # constructor
    #################################################################
    def __init__ (self, args):
        self.M = args.map                     # num of map jobs
        self.R = args.reduce                  # num of reduce jobs
        self.masterport = args.masterport     # port on which we push to workers
        self.iters = args.iters               # number of iterations
        self.metricsfile = args.metricsfile   # name of the big data file
        self.datafile = args.datafile         # name of the big data file
        self.uniquekeys = []                  # num of unique keys
        self.groups = []                      # num of groups per unique key

        self.sender4map = None                # used to send push messages
        self.sender4reduce = None             # used to send push messages
        self.rcv4barrier = None               # used to receive worker up signals
        self.rcv4map_res = None               # used to receive map results
        self.rcv4reduce_res = None            # used to receive reduce results
        
        # for the map and reduce barrier threads (to ensure the workers
        # are up or they have produced result), we maintain a thread object
        self.thr_obj_dict = {'workers_up': None, 'map_results': None, 'reduce_results': None}  
        
    # -----------------------------------------------------------------------
    # Initialize the network connections and the barriers
    def init_server (self):
        """Initialize the networking part of the server"""

        try:
            # obtain the ZeroMQ context
            context = zmq.Context().instance ()

            # Socket to send messages on. Note, we use the divide-conquer
            # pattern using PUSH and PULL
            #
            # Note that we create two such PUSH-PULL workflows:
            # one for master to map, and other for master to reduce
            #
            # It had to be done this way due to the choice of comm pattern we
            # made in Zero MQ. The * indicates any interface

            #           This is our protocol
            # base port + 0 to push args to map workers
            # base port + 1 to push args to reduce workers
            # base port + 2 to pull barrier signals indicating workers are up
            # base port + 3 to pull results from map workers
            # base port + 4 to pull results from reduce workers

            # first, the push to map workers
            self.sender4map = context.socket (zmq.PUSH)
            # set high water mark and LINGER option to ensure messages
            # get sent.
            self.sender4map.setsockopt (zmq.LINGER, -1)
            self.sender4map.setsockopt (zmq.SNDHWM, 0)
            bind_addr = "tcp://*:" + str (self.masterport)
            print("For master->map PUSH, bind addr is: ", bind_addr)
            self.sender4map.bind  (bind_addr)

            # next, the push to reduce workers
            self.sender4reduce = context.socket (zmq.PUSH)
            # set high water mark and LINGER option to ensure messages
            # get sent.
            self.sender4reduce.setsockopt (zmq.LINGER, -1)
            self.sender4reduce.setsockopt (zmq.SNDHWM, 0)
            bind_addr = "tcp://*:" + str (self.masterport+1)
            print("For master->reduce PUSH, bind addr is: ", bind_addr)
            self.sender4reduce.bind  (bind_addr)

            # next, the pull from workers to indicate they are up
            self.rcv4barrier = context.socket (zmq.PULL)
            # set high water mark as unlimited
            self.rcv4barrier.setsockopt (zmq.RCVHWM, 0)
            bind_addr = "tcp://*:" + str (self.masterport+2)
            print("For workers up->master PULL, bind addr is: ", bind_addr)
            self.rcv4barrier.bind (bind_addr)

            # next, the pull from map workers for results
            self.rcv4map_res = context.socket (zmq.PULL)
            # set high water mark as unlimited
            self.rcv4map_res.setsockopt (zmq.RCVHWM, 0)
            bind_addr = "tcp://*:" + str (self.masterport+3)
            print("For map results->master PULL, bind addr is: ", bind_addr)
            self.rcv4map_res.bind (bind_addr)

            # next, the pull from reduce workers for results
            self.rcv4reduce_res = context.socket (zmq.PULL)
            # set high water mark as unlimited
            self.rcv4reduce_res.setsockopt (zmq.RCVHWM, 0)
            bind_addr = "tcp://*:" + str (self.masterport+4)
            print("For reduce results->master PULL, bind addr is: ", bind_addr)
            self.rcv4reduce_res.bind (bind_addr)

        except:
            print("Unexpected error in init_server:", sys.exc_info()[0])
            raise

    # -----------------------------------------------------------------------
    # reset data structures for the next iteration
    def reset_master (self):
        """reset data structures"""
        self.uniquekeys = []
        self.groups = []
        
    # -----------------------------------------------------------------------
    # Create the barrier sink (either for map or reduce) as a separate process 
    # or thread. We are using the thread approach as it is easier.
    def start_barrier_sink (self, op):
        """Start a barrier thread to ensure all expected replies are received"""

        try:
            # The process-based approach of creating a child barrier process
            # is working fine. But we will use thread-based as it is easier

            #--------------- this part works fine ----------------------
            # create the apparatus to start a child process
            #operation = os.getcwd () + "/mr_barriersink.py"
            #args = ['/usr/bin/python', operation, '-p', str(self.masterport+1), barrier_cond]
            #print "MR::start_barrier_sink invoking child with args: ", args
            # start the process
            #p = sp.Popen (args)
            #-------------------------------------------------------------------

            # Note that barriers start at base port of master + 2, +3, and +4
            # for map and reduce barriers, respectively. There are three
            # possible operations; to check if the map and reduce workers
            # are up or not; and then the map and reduce results themselves.
            if (op == "workers_up"):
                # need to receive response from these many workers
                barrier_cond = self.M + self.R
                port = self.masterport + 2
                receiver = self.rcv4barrier
            elif (op == "map_results"):
                # need to receive response from these many workers
                barrier_cond = self.M
                port = self.masterport + 3
                receiver = self.rcv4map_res
            elif (op == "reduce_results"):
                # need to receive response from these many workers
                barrier_cond = self.R
                port = self.masterport + 4
                receiver = self.rcv4reduce_res
            else:
                print("bad op in starting barrier process")
                return None

            # create the args to send to the thread
            args = {'op': op, 'port': port, 'cond': barrier_cond, 'receiver': receiver}
            
            # instantiate a thread obj and start the thread
            print("MR::solve - start_barrier_sink with args: ", args)
            self.thr_obj_dict[op] = MR_Thread (barrier_sink, args)
            self.thr_obj_dict[op].start ()

        except:
            print("Unexpected error in init_server:", sys.exc_info()[0])
            raise

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes will be needed here for the Assignment
    #
    # You will need to define your own logic for splitting the energy
    # data appropriately into chunks. The current logic splits the text
    # file in approx equal sized chunks.
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

    ###################################################################
    # This method splits the initial document into almost equal sized
    # chunks and distributes it to the map workers
    ###################################################################
    def distribute_map_tasks (self):
        "create the json object to be sent to all map tasks"

        # find the file size and break it into (almost) equal sized chunks
        #
        # The way we are going to do this is that we will let the workers know
        # the start byte and the num of bytes each map task must read from
        # the original file.  To that end we first create a json obj that we
        # are going to send to the workers so that each worker can process it

        try:
            # initialize the argument data structure that we plan to send to
            # our workers. The data structure we are passing to the workers
            # comprises the name of the datafile, the starting byte from the
            # file to read and the number of bytes to read.

            # compute the size of the datafile and create (almost) equal sized
            # chunks
            doc_size = os.path.getsize (self.datafile)
            chunk_size = int (round (doc_size/self.M))  # integer division
            print("doc size = ", doc_size, ", chunk size = ", chunk_size)

            # the starting location of the next chunk to read,
            # initialized to 0 (start)
            locn2read = 0  

            # handle to the input file
            datafile = open (self.datafile,'r')
        
            # Note that we are splitting the file along bytes so it is
            # very much possible that a valid word may get split into
            # nonsensical two words but we don't care here and will treat
            # these two split parts of a word as separate unique words
            print(("MR::solve - master sending args to {} map workers:".format(self.M)))
            for i in range (self.M):
                # get the starting locn and size of the next chunk
                if (i == self.M-1): # if this is the last chunk
                    chunk_size = doc_size - locn2read

                # seek the location in the file to read from
                datafile.seek (locn2read, 0)

                # read the chunk size
                content = datafile.read (chunk_size)
                
                # create the argument to send to the task
                map_arg = {'id': i,
                           'size': chunk_size,
                           'content' : content}

                # now send this and one of the map tasks will receive it
                # according to the PUSH-PULL pattern we are using
                self.sender4map.send_json (map_arg)

                # move the starting position that many bytes ahead
                locn2read += chunk_size

            print(("MR::solve - master done sending args to {} map workers:".format(self.M)))
            # close the file after the loop
            datafile.close ()
            
        except:
            print("Unexpected error in distribute_map_tasks:", sys.exc_info()[0])
            raise

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes maybe needed here for the Assignment
    #
    # If you maintain the same design structure and the shuffle phase
    # keeps the parts ready for the reducers, then this logic will not
    # need any change
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    ###################################################################
    # This method distributes tasks to the reduce workers
    ###################################################################
    def distribute_reduce_tasks (self):
        "create the json object to be sent to all reduce tasks"

        # At the end of the shuffle phase, we had created temporary files
        # that we want to send each to each reduce worker
        try:
            print(("MR::solve - master sending args to {} reduce workers:".format(self.R)))
            for i in range (self.R):
                # Open the corresponding shuffle file for reading
                shufflefile = open ("Shuffle"+str(i)+".dat", "rb")

                # retrieve the contents of the shuffle file using pickle
                contents = pickle.load (shufflefile)

                # close the shuffle file
                shufflefile.close ()

                # send the contents to the reducer
                self.sender4reduce.send_json (contents)
        
            print(("MR::solve - master done sending args to {} reduce workers:".format(self.R)))
            
        except:
            print("Unexpected error in distribute_reduce_tasks:", sys.exc_info()[0])
            raise

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes will needed here for Assignment
    #
    # The shuffling done in this code is driven entirely by the wordcount
    # example needs. Thus, although the structure may remain similar,
    # the logic as to what it does needs to change for the energy example
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    ###########################################################
    # shuffle function. Due to the barrier synchronization,
    # only after all maps are done, we start shuffle. So there is no
    # issue about whether all maps have responded or not.
    #
    # In the shuffle phase we sort the intermediate keys and
    # depending on the number of reducers, create data sets to
    # be sent to reduce workers to handle the operation saving
    # them in temporary shuffle files each of which will be
    # handed out to the reduce workers
    #
    ###########################################################
    def shuffle_func (self):
        """ Word count shuffle function """

        print("MR::solve - Shuffle phase")

        # Since we are doing wordcount, which is an addition operation
        # we are doing combiner optimizations for per-map job
        tempfile = open ("temp.csv", "w")
        
        # for each csv file generated in the map phase, we sort the keys.
        # Note that the map csv files are numbered Map0.csv, Map1.csv, ...
        for i in range (self.M):
            # open the CVS file created by map job
            csvfile = csv.reader (open ("Map"+str(i)+".csv", "r"), delimiter=",")
            
            # get the sorted list of entries from our csv file using
            # column 1 (0-based indexing) as the key to sort on
            # and we use traditional alphabetic order
            wordlist = sorted (csvfile, key=operator.itemgetter (0))

            # Now group all entries by the unique identified words and perform
            # local combiner optimization (because it is addition, which is
            # commutative and associative). Note, again that we do not do all
            # the reduction here itself but leave it to the reduce tasks.
            groups = []
            uniquekeys = []
            for k, g in itertools.groupby (wordlist, key=operator.itemgetter (0)):
                groups.append (list (g))
                uniquekeys.append (k)

            # now for each unique key, create the data to be sent to
            # the reducers
            for j in range (len (uniquekeys)):
                tempfile.write (uniquekeys[j] + ", ")
                # note that for each unique word, we have a grouped
                # list, and each value is 1. So the length of the
                # sublist is the sum, which represents the combiner
                # optimization reduction for that unique key.
                # We save that info
                tempfile.write (str (len (groups[j])) + "\n")

            # Delete the intermediate Map file (no need for it anymore)
            os.remove ("Map"+str(i)+".csv")

        # close the temp file
        tempfile.close ()

        # Now group all the entries in sorted order so that we can hand them
        # out to reduce tasks. For that, open the temp CSV file we just
        # created
        csvfile = csv.reader (open ("temp.csv", "rt"), delimiter=",")

        # We need the following because otherwise the csv treats everything
        # as text and so our numbers are getting converted to strings
        rows = [[row[0], int(row[1])] for row in csvfile]
        
        # get the sorted list of entries from our csv file using
        # column 1 (0-based indexing used here) as the key to sort on
        # and we use traditional alphabetic order.  
        wordlist = sorted (rows, key=operator.itemgetter (0))
        
        # Identify the total number of unique keys we have in our data
        # We need this info to make a split in the Shuffle file to hand out
        # to reduce tasks. We create as many intermediate shuffle files
        # as the number of readers we have
        for k, g in itertools.groupby (wordlist, key=operator.itemgetter (0)):
            self.groups.append (list (g))
            self.uniquekeys.append (k)

        print("MR::shuffle - Total unique keys = ", len (self.uniquekeys))

        # now save the shuffle info into as many files as the number
        # of reduce jobs
        keysPerReduce = int (round (len(self.uniquekeys)/self.R))  # integer division
        keysConsumed = 0  # initial condition

        for i in range (self.R):
            # we open the file with binary write property since
            # we are going to write an in-memory representation
            # of the lists of lists we have stored in self.groups
            shufflefile = open ("Shuffle" + str (i) + ".dat", "wb")

            # get the starting locn and num of entries to be stored
            # in this shuffle file.
            if (i == self.R-1): # if this is the last reduce task
                    keysPerReduce = len(self.uniquekeys) - keysConsumed

            # use pickle to store binary representation.
            pickle.dump (self.groups[keysConsumed:keysConsumed+keysPerReduce], shufflefile, pickle.HIGHEST_PROTOCOL)
            # update the starting locn for the next shuffle file.
            keysConsumed = keysConsumed + keysPerReduce

            # close this shuffle file
            shufflefile.close ()

        # cleanup the temp file
        os.remove ("temp.csv")
        

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes may not be needed here for the Assignment
    #
    # Unless there is a substantial difference in how the results from
    # workers are combined, this logic may not need any change
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    ###################################################################
    #
    # finalize function. We are assured that there are no
    # remaining reduce jobs because we use the barrier synchronization
    #
    # Here we assemble all the results produced by the reduce workers
    # and combine them into a single results file
    ###################################################################
    def finalize_func (self):
        """ Word count finalize function """
        print("Finalize phase: Aggregate all the results")

        # effectively, we go thru all the reduce results files and
        # get the results
        results = open ("results.csv", "w")
        for i in range (self.R):
            reduce_file = open ("Reduce"+str(i)+".csv", "rt")
            data = reduce_file.read ()
            reduce_file.close ()
            results.write (data)
            
        results.close ()

        # cleanup. Delete all reducer related files
        for i in range (self.R):
            os.remove ("Shuffle"+str(i)+".dat")
            os.remove ("Reduce"+str(i)+".csv")

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes may not be needed here for the Assignment
    #
    # I do not think any change is needed here because it just captures
    # the basic workflow.
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    #####################################################################
    # The method that solves the problem using the map reduce approach
    # This is the main "driver" function for the master
    #####################################################################
    def run_iter (self, mf):
        """Run one iteration of the problem using map reduce"""

        try:
            ############  the real work starts now ###########

            print ("****** Next iteration of map-reduce ************")
            
            # we keep track of the total running time
            total_running_time = 0
            
            ########### Phase 1: Map ###################

            print("***** Starting Map Phase ***********")
            # start the timing measurement
            start_time = time.time ()

            # start the map barrier sink again to receive the ACKs after
            # each map worker has completed its work.
            print("MR::solve - start map results barrier")
            self.start_barrier_sink ("map_results")
            
            ### distribute tasks to all map workers ###
            print("MR::solve - send message to map workers")
            self.distribute_map_tasks ()
            
            ### barrier ###
            print("MR::solve - wait for map sink to return")
            self.thr_obj_dict["map_results"].join ()

            # this part works if we use the process approach. But we are
            # using the simpler thread-based approach
            #retcode = handle.wait ()
            #print "MR::solve - barrier returned with status: ", retcode
            
            # stop the timing measurement
            end_time = time.time ()

            map_phase_time = end_time-start_time
            print("***** Map phase required: ", map_phase_time, " seconds")
            total_running_time = total_running_time + map_phase_time

            ########### Phase 2: Shuffle ###################
            # this is not done in parallel for our case. In reality shuffle
            # will do the necessary things in parallel and move things around
            # so that the reduce job can fetch the right things from the right
            # place. We do not have any such elaborate mechanism.

            # start the timing measurement
            start_time = time.time ()

            # invoke the shuffle logic
            print("***** Starting Shuffle Phase ***********")
            self.shuffle_func ()

            # stop the timing measurement
            end_time = time.time ()

            shuffle_phase_time = end_time - start_time
            print("***** Shuffle phase required: ", shuffle_phase_time, " seconds")
            total_running_time = total_running_time + shuffle_phase_time

            ########### Phase 3: Reduce ###################

            print("***** Starting Reduce Phase ***********")
            # start the timing measurement
            start_time = time.time ()

            # start the reduce barrier sink again to receive the ACKs after
            # each reduce worker has completed its work.
            print("MR::solve - start reduce barrier")
            self.start_barrier_sink ("reduce_results")
            
            ### distribute tasks to all workers ###
            print("MR::solve - send message to reduce workers")
            self.distribute_reduce_tasks ()
            
            ### barrier ###
            print("MR::solve - wait for reduce sink to return")
            self.thr_obj_dict["reduce_results"].join ()

            # this part works if we use the process approach
            #retcode = handle.wait ()
            #print "MR::solve - barrier returned with status: ", retcode

            # stop the timing measurement
            end_time = time.time ()

            reduce_phase_time = end_time - start_time
            print("***** Reduce phase required: ", reduce_phase_time, " seconds")
            total_running_time = total_running_time + reduce_phase_time

            ########### Phase 4: Finalize ###################

            print("***** Starting Finalize Phase ***********")
            # start the timing measurement
            start_time = time.time ()

            # finalize the results
            self.finalize_func ()
            
            # stop the timing measurement
            end_time = time.time ()

            finalize_phase_time = end_time - start_time
            print("***** Finalize phase required: ", finalize_phase_time, " seconds")
            total_running_time = total_running_time + finalize_phase_time

            print("*** Total Running Time for wordcount ***  = ",  total_running_time)

            # write this information into the file
            mf.write (str(map_phase_time) + ", " + str(shuffle_phase_time) + ", " + str(reduce_phase_time) + ", " + str(finalize_phase_time) + ", " + str(total_running_time) + "\n")
            mf.flush ()
            
        except:
            print("Unexpected error in run_iter method:", sys.exc_info()[0])
            raise

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # @NOTE@: changes may not be needed here for the Assignment
    #
    # I do not think any change is needed here because it just captures
    # the basic workflow.
    #
    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    #####################################################################
    # The method that solves the problem using the map reduce approach
    # This is the main "driver" function for the master
    #####################################################################
    def solve (self):
        """Solve the problem using map reduce"""

        try:
            ############ initialization ##################
            # here we start our end of the PUSH based workflow
            print("MR::solve - initialize server")
            self.init_server ()
            
            # start a barrier to make sure the required number of map
            # and reduce workers are up and running
            print("MR::solve - start barrier to make sure all our map and reduce workers are up")
            self.start_barrier_sink ("workers_up")

            # the join below results in a barrier synchronization. We create
            # threads per map and reduce workers and wait to hear from them
            #
            # note that the self.thr_obj_dict data structure is a dictionary
            # comprising name-values. The name is map and reduce; their
            # values are the number of map and reduce workers.
            print("MR::solve - wait for workers to start")
            self.thr_obj_dict["workers_up"].join ()

            print("MR::solve - All map and reduce workers are up")
            print("***** Initialization phase completed")

            # handle to the output file
            metricsfile = open (self.metricsfile,'w')
            metricsfile.write("Map       Shuffle        Reduce          Finalize        Total\n")
            # run the iterations
            for i in range (self.iters):
                # run the next iter
                self.run_iter (metricsfile)
                # reset the data structures for the next iter
                self.reset_master ()
                # some rest :-)
                time.sleep (5)

            # close the metrics collection file
            metricsfile.close ()

        except:
            print("Unexpected error in solve method:", sys.exc_info()[0])
            raise
