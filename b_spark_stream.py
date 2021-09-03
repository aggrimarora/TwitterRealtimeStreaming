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

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import nltk
nltk.downloader.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
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
dataStream = ssc.socketTextStream("twitter",9009)
sia = SIA()

track = {
    'covid': ['covid', 'vaccine', 'moderna', 'seconddose', 'vaccinated', 'pfizer', 'astrazeneca', 'deltavariant', 'novaccinepassports', 'covid19'],
    'football': ['portugal', 'ronaldo', 'fernando', 'danilo', 'renato', 'william', 'havertz', 'champ', 'semedo', 'jota', 'benzema', 'portugues', 'hungary'],
    'june': ['juneteenth', 'juneteenthcelebration', 'juneteenth2021', 'fathersday', 'father', 'dad',
             'happyfathersday', 'juneteenthday', '#JuneteenthFederalHoliday', 'FathersDay2021'],
    'pride': ['pride', 'pridemonth', 'gay', 'lgbtq', 'love', 'loveislove','queer', 'biseexual', 'lesbian', 'lgbtpride', 'trans'],
    'basketball': ['booker', 'nba', 'suns', 'LACvsPHX', 'clippers', 'kobe', 'nbaplayoffs', 'ThatsGame', 'laclippers', 'clippernation']
}

# get the sentiment for each filtered tweet
def get_tweet_sentiment(tweet):
    pol = sia.polarity_scores(tweet)
    if pol['compound'] > 0.3:
        return 'pos'
    elif pol['compound'] < -0.3:
        return 'neg'
    else:
        return 'neu'

# filter tweets based on the topics we're tracking
def filter_tweets(tweet):
    for word in tweet.split(" "):
        for tags in track.values():
            if word.strip('#').lower() in tags:
                return True
    return False

# get the parent topic of the tweet
def get_tweet_topic(tweet):
    for tag in [x.strip('#').lower() for x in tweet.split(" ")]:
        for key in track.keys():
            for val in track[key]:
                if val == tag.lower():
                    return key
    return ""

filtered_tweets = dataStream.filter(filter_tweets)
# create three unique keys for the three sentiments for a topic({topic}-{pos, neg, neu} \t #count)
topics_count = filtered_tweets.map(lambda x: (get_tweet_topic(x) + ':' + get_tweet_sentiment(x),1))

# checks if graph txt file exists and removes it before running
if os.path.exists('sentiment_output_graph.txt'):
    os.remove('sentiment_output_graph.txt')

# creates new files for graph and output, and appends to them
output = open('sentiment_output_data.txt', 'w+')

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
topics_total = topics_count.updateStateByKey(aggregate_tags_count)

# process a single time interval
def process_interval(time, rdd):
    # print a separator    
    print("----------- %s -----------" % str(time))
    output.write("----------- %s -----------\n" % str(time))
    try:
        records = rdd.collect()
        output_graph = open('sentiment_output_graph.txt', 'w+')
        for tag in records:
            print('{:<40} {}'.format(tag[0], tag[1]))
            output_graph.write('{:<40} {}\n'.format(tag[0], tag[1]))
            output_graph.truncate()
            output.write('{:<40} {}\n'.format(tag[0], tag[1]))
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# do this for every single interval
topics_total.foreachRDD(process_interval)



# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

output_graph.close()
output.close()
