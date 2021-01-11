from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('MinTemperature')
sc = SparkContext(conf=conf)


def parse_line(ln):
    fields = ln.split(',')
    stationID = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1
    return stationID, entry_type, temperature


def parse_lines():
    lines = sc.textFile('../data/1800.csv')
    parsed_lines = lines.map(parse_line)
    return parsed_lines


def get_min_temp():
    parsed_lines = parse_lines()
    min_temps = parsed_lines.filter(lambda x: 'TMIN' in x[1])
    station_temp = min_temps.map(lambda x: (x[0], x[2]))
    results = station_temp.reduceByKey(lambda x, y: min(x, y)).collect()
    return results


def get_max_temp():
    parsed_lines = parse_lines()
    max_temps = parsed_lines.filter(lambda x: 'TMAX' in x[1])
    station_temp = max_temps.map(lambda x: (x[0], x[2]))
    results = station_temp.reduceByKey(lambda x, y: max(x, y)).collect()
    return results


def print_results(results):
    for res in results:
        print(res[0] + "\t{:.2f}C".format(res[1]))


print('MIN TEMP')
print_results(get_min_temp())

print('MAX TEMP')
print_results(get_max_temp())
