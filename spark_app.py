"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.
    To execute this in a Docker container, do:

        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415
    and inside the docker:
        spark-submit spark_app.py
    For more instructions on how to run, refer to final tutorial 8 slides.
    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat
"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests
import os

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter", 9009)

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

# 5 related #hashtags
required_hashtags = ['#COVID19', '#NoVaccinePassports', '#DeltaVariant', '#CovidVaccine', '#vaccines']

# filter the words to get only hashtags
hashtags = words.filter(lambda w: w in required_hashtags)

s = hashtags.map(lambda x: x.strip("#").lower())
s = s.filter(lambda x: x not in [])

# set(lst1) & set(lst2)

# map each hashtag to be a pair of (hashtag,1)
hashtag_counts = s.map(lambda x: (x, 1))


# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)

# checks if graph txt file exists and removes it before running
if os.path.exists('output_graph.txt'):
    os.remove('output_graph.txt')

# creates new files for graph and output, and appends to them
output = open('part_A_output_data.txt', 'a+')


# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    output.write("----------- %s -----------\n" % str(time))
    try:
        # sort counts (desc) in this time instance and take top 10
        sorted_rdd = rdd.sortBy(lambda x: x[1], False)
        tags = sorted_rdd.take(10)
        output_graph = open('output_graph.txt', 'w+')
        # print it nicely
        for tag in tags:
            output_graph.write('{:<40} {}\n'.format(tag[0], tag[1]))
            output.write('{:<40} {}\n'.format(tag[0], tag[1]))
            print('{:<40} {}'.format(tag[0], tag[1]))
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# do this for every single interval
hashtag_totals.foreachRDD(process_interval)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

output_graph.close()
output.close()
