from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('RatingHistogram')
sc = SparkContext(conf=conf)


def parse_line(ln):
    fields = ln.split(',')
    customer_id = int(fields[0])
    amount = float(fields[2])
    return customer_id, amount


lines = sc.textFile('../data/myFile0.csv')
parsed_lines = lines.map(parse_line)
customers = parsed_lines.reduceByKey(lambda x, y: x + y).map(lambda x_y: (x_y[1], x_y[0])).sortByKey()
results = customers.collect()

for res in results:
    print('customer: {1}\t\t({0}$)'.format(res[0], res[1]))
