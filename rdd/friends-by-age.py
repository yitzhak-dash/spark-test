from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('RatingHistogram')
sc = SparkContext(conf=conf)


def parse_line(ln):
    fields = ln.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


lines = sc.textFile('./data/fakefriends.csv')
rdd = lines.map(parse_line)
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
results = average_by_age.collect()

for res in results:
    print(res)
