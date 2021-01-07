from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('RatingHistogram')
sc = SparkContext(conf=conf)

input = sc.textFile('./data/Book')
words = input.flatMap(lambda x: x.split())
words_counts = words.countByValue()

for word, count in words_counts.items():
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print(clean_word, count)
