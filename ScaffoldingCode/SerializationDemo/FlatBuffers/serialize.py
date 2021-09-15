#  Author: Aniruddha Gokhale
#  Created: Fall 2021
#  (based on code developed for Distributed Systems course in Fall 2019)
#
#  Purpose: demonstrate serialization of topic data type using flatbuffers
#
#  Here our topic comprises a sequence number, a timestamp, and a data buffer
#  of several uint32 numbers (whose value is not relevant to us)

import os
import sys

# this is needed to tell python where to find the flatbuffers package
# make sure to change this path to where you have compiled and installed
# flatbuffers.  If the python package is installed in your system wide files
# or virtualenv, then this may not be needed
sys.path.append(os.path.join (os.path.dirname(__file__), '/home/gokhale/Apps/flatbuffers/python'))
import flatbuffers    # this is the flatbuffers package we import
import time   # we need this get current time
import numpy as np  # to use in our vector field

import MyKafkaTopic.Topic as mkt   # this is the generated code by the flatc compiler

# This is the method we will invoke from our driver program
def serialize (seq_num, name, vec_len):
    # first obtain the builder object
    builder = flatbuffers.Builder (0);

    # create the name string for the name field using
    # the parameter we passed
    name_field = builder.CreateString (name)
    
    # create the array that we will add to our buffer
    mkt.StartDataVector (builder, vec_len)
    for i in reversed (range (vec_len)):
        builder.PrependUint32 (i)
    data = builder.EndVector ()
    
    # let us create a topic and add contents to it.
    # Our topic consists of a seq num, timestamp, and an array of uint32s
    mkt.Start (builder)  # serialization starts with the "Start" method
    mkt.AddSeqNo (builder, seq_num)
    mkt.AddTs (builder, time.time ())   # serialize current timestamp
    mkt.AddName (builder, name_field)  # serialize the name
    mkt.AddData (builder, data)  # serialize the dummy data
    topic = mkt.End (builder)  # get the topic of all these fields

    # end the serialization process
    builder.Finish (topic)

    # get the serialized buffer
    buf = builder.Output ()

    return buf

# deserialize the incoming serialized structure into native data type
def deserialize (buf):
    recvd_topic = mkt.Topic.GetRootAs (buf, 0)

    # sequence number
    print ("Sequence num = {}".format (recvd_topic.SeqNo ()))

    # topic name received
    print ("received name = {}".format (recvd_topic.Name ()))

    # received vector data
    print ("Received vector has {} elements".format (recvd_topic.DataLength ()))
    print ("Received vector as numpy {}".format (recvd_topic.DataAsNumpy ()))
    
