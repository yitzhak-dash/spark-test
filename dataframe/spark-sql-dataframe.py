from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema', 'true').csv('./data/fakefriends-header.csv')

print('show schema')
people.printSchema()

print('show name')
people.select('name').show()

print('filter by age')
people.filter(people.age < 21).show()

print('group by age')
people.groupBy('age').count().show()

print('select name, age +10')
people.select(people.name, people.age + 10).show()

spark.stop()
