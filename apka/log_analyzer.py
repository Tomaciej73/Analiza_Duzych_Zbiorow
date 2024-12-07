from pyspark import SparkContext, SparkConf
import apache_access_log
import analysis_plots
import sys

conf = SparkConf().setAppName("Analizator dzienników")
sc = SparkContext(conf=conf)
#sc.stop()
logFile = 'apache_log_example.log'

access_logs = (sc.textFile(logFile)
               .map(apache_access_log.parse_apache_log_line)
               .cache())

# Oblicz statystyki na podstawie rozmiaru treści
content_sizes = access_logs.map(lambda log: log.content_size).cache()
content_list = []
content_list.append(content_sizes.reduce(lambda a, b : a + b) / content_sizes.count())
content_list.append(content_sizes.min())
content_list.append(content_sizes.max())
print ("Średni rozmiar treści: \n%i, \nMin: %i, \nMax: %s" % (
    content_list[0],content_list[1],content_list[2]
    ))
analysis_plots.content_analysis_plot(content_list)

responseCodeToCount = (access_logs.map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .take(100))
print("Liczba kodów odpowiedzi:\n%s" % '\n'.join(map(str, responseCodeToCount)))

ipAddresses = (access_logs
               .map(lambda log: (log.ip_address, 1))
               .reduceByKey(lambda a, b : a + b)
               .filter(lambda s: s[1] > 10)
               .map(lambda s: s[0])
               .take(100))
print("Adresy IP, z których uzyskano dostęp więcej niż 10 razy:\n%s" % '\n'.join(ipAddresses))

# Najważniejsze punkty końcowe
topEndpoints = (access_logs
                .map(lambda log: (log.endpoint, 1))
                .reduceByKey(lambda a, b : a + b)
                .takeOrdered(10, lambda s: -1 * s[1]))
print("Najważniejsze punkty końcowe:\n%s" % '\n'.join(map(str, topEndpoints)))


