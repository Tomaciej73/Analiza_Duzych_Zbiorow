from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat_ws
import apache_access_log
import analysis_plots
import sys

from log_analyzer import content_list
if SparkContext._active_spark_context:
    SparkContext._active_spark_context.stop()

conf = SparkConf().setAppName("Analizator dzienników SQL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

logFile = 'apache_log_example.log'

access_logs = (sc.textFile(logFile)
               .map(apache_access_log.parse_apache_log_line)
               .cache())

schema_access_logs = sqlContext.createDataFrame(access_logs)
schema_access_logs = schema_access_logs.withColumn(
    "date_time", concat_ws(" ", schema_access_logs["date"], schema_access_logs["time"])
)
schema_access_logs.registerTempTable("logs")


# 1. 10 najlepszych punktów końcowych na podstawie rozmiaru zawartości
topEndpointsMaxSize = (sqlContext
                .sql("SELECT endpoint, content_size/1024 AS size_mb FROM logs ORDER BY size_mb DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
analysis_plots.bar_plot_list_of_tuples_horizontal(
    topEndpointsMaxSize,
    'Przepływ danych – MB',
    'Punkty końcowe',
    'Analiza punktów końcowych na podstawie maksymalnego rozmiaru zawartości'
)

# 2. Kody odpowiedzi HTTP
responseCodeToCount = (sqlContext
                       .sql("SELECT response_code, COUNT(*) AS theCount FROM logs GROUP BY response_code")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
analysis_plots.bar_plot_list_of_tuples(
    responseCodeToCount,
    'Kody odpowiedzi',
    'Liczba kodów',
    'Analiza kodów odpowiedzi'
)


# 3. Najczęstsze IP z więcej niż 10 trafieniami
frequentIpAddressesHits = (sqlContext
               .sql("SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10 LIMIT 100")
               .rdd.map(lambda row: (row[0], row[1]))
               .collect())
analysis_plots.bar_plot_list_of_tuples_horizontal(
    frequentIpAddressesHits,
    'Liczba trafień',
    'Adres IP',
    'Najczęstsi użytkownicy (częste trafienia adresu IP)'
)

topEndpointsFiltered = (sqlContext
    .sql("""
        SELECT endpoint, COUNT(*) AS total
        FROM logs
        WHERE endpoint NOT LIKE '%.ico'
          AND endpoint NOT LIKE '%.png'
          AND endpoint NOT LIKE '%.gif'
        GROUP BY endpoint
        ORDER BY total DESC
        LIMIT 10
    """)
    .rdd.map(lambda row: (row[0], row[1]))
    .collect())

analysis_plots.bar_plot_list_of_tuples_horizontal(
    topEndpointsFiltered,
    'Liczba uzyskanych dostępów',
    'Punkty końcowe',
    'Najczęściej występujące punkty końcowe'
)


Day = '07/Mar/2004'
trafficperDay = (sqlContext
                       .sql("SELECT time,content_size/1024 FROM logs where date='08/Mar/2004'")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
analysis_plots.time_series_plot(trafficperDay,Day,'Rozmiar zawartości - MB','Analiza ruchu(dzień)')

# 4. Ruch w czasie
trafficWithTime = (sqlContext
                   .sql("SELECT date_time, content_size/1024 AS size_mb FROM logs")
                   .rdd.map(lambda row: (row[0], row[1]))
                   .collect())
print("Ruch w czasie:")
for entry in trafficWithTime:
    print(entry)
analysis_plots.time_series_plot(
    trafficWithTime,
    'Czas',
    'Rozmiar zawartości – MB',
    'Analiza ruchu w czasie'
)

analysis_plots.time_series_plot(trafficWithTime)

trafficWithTime = (sqlContext
                       .sql("SELECT date_time, content_size/1024 FROM logs")
                       .rdd.map(lambda row: (row[0], row[1]))
                       .collect())
print ("Ruch w czasie: %s" % (trafficWithTime))
#time_series_plot(trafficWithTime)


# 5. 10 najczęstszych żądań 404
NotFoundRequests = (sqlContext
                .sql("SELECT endpoint, date_time FROM logs WHERE response_code='404' ORDER BY date_time DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
analysis_plots.bar_plot_list_of_tuples(
    NotFoundRequests,
    'Punkty końcowe',
    'Czas',
    '10 najczęstszych żądań 404'
)


# 6. Statystyki rozmiaru treści
content_size_stats = (sqlContext
                      .sql("SELECT SUM(content_size), COUNT(*), MIN(content_size), MAX(content_size) FROM logs")
                      .first())
print("Statystyki rozmiaru treści:")
print(f"Średni rozmiar: {content_size_stats[0] / content_size_stats[1]:.2f} MB, "
      f"Min: {content_size_stats[2]} B, Max: {content_size_stats[3]} B")


# 7. Adresy IP z więcej niż 10 trafieniami
ipAddresses = (sqlContext
               .sql("SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10 LIMIT 100")
               .rdd.map(lambda row: row[0])
               .collect())
print ("Wszystkie adresy IP > 10 razy: %s" % ipAddresses)
for ip in ipAddresses:
    print(ip)

# 8. Najważniejsze punkty końcowe
topEndpoints = (sqlContext
                .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .rdd.map(lambda row: (row[0], row[1]))
                .collect())
print ("Najważniejsze punkty końcowe: %s" % (topEndpoints))
for endpoint in topEndpoints:
    print(endpoint)