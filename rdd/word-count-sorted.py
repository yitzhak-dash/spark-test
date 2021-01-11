import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('RatingHistogram')
sc = SparkContext(conf=conf)


def normalize(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


input = sc.textFile('../data/Book')
words = input.flatMap(normalize)
# this a hard way to make count of value
# convert each value into key/value pair with a value of 1
# count values up
words_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# flip the (word, count) to (count, word)
words_count_sorted = words_counts.map(lambda x_y: (x_y[1], x_y[0])).sortByKey()
results = words_count_sorted.collect()

for item in results:
    count = str(item[0])
    clean_word = str(item[1].encode('ascii', 'ignore'))
    if clean_word:
        print(clean_word + ':\t\t' + count)
