from pyspark.sql import SparkSession
from grafana_stream_listener import GrafanaStreamListener, GrafanaStreamListenerManager
from influx_writer_client import InfluxWriter
import atexit
import os

# Provide the jars in --jars cli argument of spark-submit

app_name = "Grafana Streaming POC"
spark = SparkSession.builder.appName(app_name) \
    .config("spark.sql.streaming.streamingQueryListeners",
            "org.dataschool.listener.GrafanaStreamingQueryListener").getOrCreate()

sc = spark.sparkContext

# Start the Py4j Callback server
py4j_gateway = sc._gateway
py4j_gateway.start_callback_server()

# Influx writer creation
org = "<your influx db org>"
url = "<your influx db url>"
token = or os.environ.get("INFLUXDB_TOKEN")
bucket = "<your influx db bucket"
influx_writer = InfluxWriter(org, url, bucket, token)

# Grafana Stream Listener
listener = GrafanaStreamListener(app_name, influx_writer)

GrafanaStreamListenerManager.register(listener)

input_df = spark.readStream.format("rate").option("rowsPerSecond", 10000).load()

query_handler = input_df.writeStream.format("console").outputMode("append").queryName("Grafana_RateStream").trigger(
    processingTime="0.5 seconds").start()


def exit_handler():
    print('Existing the application')
    py4j_gateway.shutdown_callback_server()  # on exit


atexit.register(exit_handler)
query_handler.awaitTermination()
