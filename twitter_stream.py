# Download VADER for sentiment analysis
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
nltk.download('vader_lexicon')

# Load helper methods
from load_saved_model import load_model
from save_tweets import save_tweet_scores_by_date

from datetime import datetime
today = datetime.today().strftime('%Y-%m-%d')

# initialize sentiment analyzer
sid = SentimentIntensityAnalyzer()

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# load latest model
model = load_model()
# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)
# compute sentiment analysis scores
dataStream = dataStream.map(lambda x: sid.polarity_scores(x))
# output prediction
dataStream.map(lambda x: model.transform(x)).pprint()
# save tweet scores to filesystem
dataStream.map(lambda x: save_tweet_scores_by_date(x, today))

ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()


