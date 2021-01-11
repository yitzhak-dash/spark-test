from pyspark.sql import SparkSession, Row, functions

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema', 'true').csv('./data/fakefriends-header.csv')

people.select('age', 'friends').groupBy('age').avg('friends').sort('age').show()

print("rounded result")
people.select('age', 'friends').groupBy('age').agg(functions.round(functions.avg("friends"), 2).alias("friends_avg")) \
    .sort('age').show()

spark.stop()
