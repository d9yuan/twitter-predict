import yaml

config = yaml.read(open('./config.yml'))

API_KEY = config['auth']['alphavantage']['API_KEY']

from pyspark.ml.feature import VectorAssembler
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext(appName="UpdateModel", master="local[*]")
spark = SparkSession(sc)

from datetime import datetime
today = datetime.today().strftime('%Y-%m-%d')

historical_data = sc.read.option("header", True).format("csv").load("./historical_news_price_data.csv")
today_tweets_scores = sc.read.option("header", True).format("csv").load(f"./tweets_price_data_${today}.csv")

# retrieve today's Bitcoin prices
import requests
url = f'https://www.alphavantage.co/query?date={today}&function=DIGITAL_CURRENCY_DAILY&symbol=BTC&market=USD&apikey={API_KEY}'
r = requests.get(url)
price = r.json()['Time Series (Digital Currency Daily)']

# append today's price to today's tweets df
from pyspark.sql.functions import lit
today_tweets_scores = today_tweets_scores.withColumn('DayDifference', lit(price['4a. close (USD)'] - price['1a. open (USD)']))

# add today's data to historical_data and retrain the model
all_data = today_tweets_scores.union(historical_data)

# creating vectors from features
# Apache MLlib takes input if vector form
assembler = VectorAssembler(inputCols=['Compound Score',
                                       'Positive Score',
                                       'Neutral Score',
                                       'Negative Score', ], outputCol='Scores')
final_data = assembler.transform(all_data)

# if two seperate dataframes, don't need to do any split, just final_data[["Scores", "DayDifference"]]
splits = final_data[["Scores", "DayDifference"]].randomSplit([0.8, 0.2])
train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression
lin_reg = LinearRegression(featuresCol="Scores", labelCol="DayDifference")
linear_model = lin_reg.fit(train_df)

# evaluating model trained for Rsquared error
results = linear_model.evaluate(train_df)

print("Coefficients: " + str(linear_model.coefficients))
print("\nIntercept: " + str(linear_model.intercept))

print('Rsquared :', results.r2)

# save the updated model
from load_saved_model import save_model
save_model(linear_model.coefficients, linear_model.intercept)


