from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('RatingHistogram')
sc = SparkContext(conf=conf)

lines = sc.textFile('./data/ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResult = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResult.items():
    print("%s %i" % (key, value))
