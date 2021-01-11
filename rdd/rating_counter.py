# import os
#
# os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('RatingHistogram')
sc = SparkContext(conf=conf)

lines = sc.textFile('../data/ml-100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResult = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResult.items():
    print("%s %i" % (key, value))

print("SparkContext version:\t", sc.version)  # return SparkContext version
print("python version:\t", sc.pythonVer)  # return python version
print("master URL:\t", sc.master)  # master URL to connect to
print("path where spark is installed on worker nodes:\t", sc.sparkHome)  # path where spark is installed on worker nodes
print("name of spark user running SparkContext:\t", sc.sparkUser())  # name of spark user running SparkContext
