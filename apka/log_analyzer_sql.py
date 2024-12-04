from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import apache_access_log
import analysis_plots
import sys

from log_analyzer import content_list

conf = SparkConf().setAppName("Analizator dzienników SQL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

logFile = 'apache_log_example.log'


access_logs = (sc.textFile(logFile)
               .map(apache_access_log.parse_apache_log_line)
               .cache())

schema_access_logs = sqlContext.createDataFrame(access_logs)
schema_access_logs.registerTempTable("logs")


#10 najlepszych punktów końcowych, które przesyłają najwięcej treści
topEndpointsMaxSize = (sqlContext
                .sql("SELECT endpoint,content_size/1024 FROM logs ORDER BY content_size DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())

analysis_plots.bar_plot_list_of_tuples_horizontal(topEndpointsMaxSize, 'Przepływ danych – MB', 'Punkty końcowe', 'Analiza punktów końcowych na podstawie maksymalnego rozmiaru zawartości')



responseCodeToCount = (sqlContext
                       .sql("SELECT response_code, COUNT(*) AS theCount FROM logs GROUP BY response_code")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
analysis_plots.bar_plot_list_of_tuples(responseCodeToCount,'Kody odpowiedzi', 'Liczba kodów', 'Analiza kodów odpowiedzi')

# Najczęściej odwiedzający (najczęstsze wizyty z adresu IP).
frequentIpAddressesHits = (sqlContext
               .sql("SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10 LIMIT 100")
               .rdd.map(lambda row: (row[0], row[1]))
               .collect())
analysis_plots.bar_plot_list_of_tuples_horizontal(frequentIpAddressesHits,'Liczba trafień', 'Adres IP', 'Najczęstsi użytkownicy (częste trafienia adresu IP)')

topEndpoints = (sqlContext
                .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
analysis_plots.bar_plot_list_of_tuples_horizontal(topEndpoints,'Liczba uzyskanych dostępów', 'Punkty końcowe', 'Najczęściej występujące punkty końcowe')


Day = '07/Mar/2004'
trafficperDay = (sqlContext
                       .sql("SELECT time,content_size/1024 FROM logs where date='08/Mar/2004'")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
analysis_plots.time_series_plot(trafficperDay,Day,'Rozmiar zawartości - MB','Analiza ruchu(dzień)')

trafficWithTime = (sqlContext
                       .sql("SELECT date_time, content_size/1024 FROM logs")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
print ("Ruch w czasie: %s" % (trafficWithTime))
analysis_plots.time_series_plot(trafficWithTime)

trafficWithTime = (sqlContext
                       .sql("SELECT date_time, content_size/1024 FROM logs")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
print ("Ruch w czasie: %s" % (trafficWithTime))
#time_series_plot(trafficWithTime)


#10 najczęstszych żądań 404 z ich punktami końcowymi i czasem
NotFoundRequests = (sqlContext
                .sql("SELECT endpoint,date_time FROM logs where response_code='404' ORDER BY date_time DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
analysis_plots.bar_plot_list_of_tuples(NotFoundRequests,'Złe żądanie','Data-Godzina','404 Analiza błędnego żądania')


# Oblicz statystyki na podstawie rozmiaru treści.
content_size_stats = (sqlContext
                      .sql("SELECT SUM(content_size),COUNT(*),MIN(content_size), MAX(content_size) FROM logs")
                      .first())                     
print( "Średni rozmiar treści: %i, Min: %i, Max: %s" % (
    content_size_stats[0] / content_size_stats[1],
    content_size_stats[2],
    content_size_stats[3]
))
analysis_plots.content_analysis_plot(content_list)

# Dowolny adres IP, który uzyskał dostęp do serwera więcej niż 10 razy.
ipAddresses = (sqlContext
               .sql("SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10 LIMIT 100")
               .rdd.map(lambda row: row[0])
               .collect())
print ("Wszystkie adresy IP > 10 razy: %s" % ipAddresses)

# Najważniejsze punkty końcowe
topEndpoints = (sqlContext
                .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
print ("Najważniejsze punkty końcowe: %s" % (topEndpoints))
