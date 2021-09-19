#  Author: Aniruddha Gokhale
#  Created: Fall 2021
#  
#
#  Purpose: demonstrate serialization of topic data type using protobuf
#
#  Here our topic comprises a sequence number, a timestamp, and a data buffer
#  of several uint32 numbers (whose value is not relevant to us)

import os
import sys

import time   # we need this get current time
import numpy as np  # to use in our vector field

from MyKafkaTopic import schema_pb2  # this is the generated code by the protoc compiler

# This is the method we will invoke from our driver program
def serialize (seq_num, name, vec_len):

    # first get a handle to the message, in our case the Topic message
    pb = schema_pb2.Topic ()

    # now insert individual fields
    pb.seq_num = seq_num  # insert the seq num
    pb.ts = time.time ()   # insert current time
    pb.name = name # insert name

    # now insert elements in the vector (the repeated entry)
    # Note, these are dynamically sized arrays
    for i in range (vec_len):
        pb.data.append (i)   # insert the loop index

    # print what we are serializing
    print ("Serializing the following buffer")
    print ("\tSequence num = {}".format (pb.seq_num))
    print ("\tTimestamp = {}".format (pb.ts))
    print ("\tName = {}".format (pb.name))
    print ("\tData is {}".format (pb.data))

    # now get the stringified representation
    buf = pb.SerializeToString ()
    
    return buf

# deserialize the incoming serialized structure into native data type
def deserialize (buf):

    # first get a handle to the message, in our case the Topic message
    pb = schema_pb2.Topic ()

    # now parse the serialized buffer
    pb.ParseFromString (buf)

    # now we retrieve the fields from the serialized buffer
    print ("Deserialized buffer")

    # sequence number
    print ("\tSequence num = {}".format (pb.seq_num))

    # technically we should check if optional fields exist using the HasField ()
    # check but we skip it here because we know we have sent it.
    
    # time stamp 
    print ("\tReceived timestamp = {}".format (pb.ts))

    # topic name received
    print ("\tReceived name = {}".format (pb.name))

    # received vector data
    print ("\tReceived vector is {}".format (pb.data))
    
