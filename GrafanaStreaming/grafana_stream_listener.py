from pyspark import SparkContext
from influx_writer_client import QueryProgressEvent
import json


class GrafanaStreamListenerManager:
    """
    Wrapper to get access to GrafanaStreamListenerManager JVM process and then access the GrafanaStreamListenerManager class object
    """
    package = "org.dataschool"
    subpackage = "listener"
    manager = "GrafanaStreamingQueryListenerManager"

    @staticmethod
    def get_manager():
        jvm = SparkContext._active_spark_context._jvm
        manager = getattr(jvm,
                          f"{GrafanaStreamListenerManager.package}.{GrafanaStreamListenerManager.subpackage}.{GrafanaStreamListenerManager.manager}")

        return manager

    @staticmethod
    def register(listener):
        manager = GrafanaStreamListenerManager.get_manager()
        uuid = manager.register(listener)
        return uuid

    @staticmethod
    def unregister(uuid):
        manager = GrafanaStreamListenerManager.get_manager()
        manager.unregister(uuid)


class GrafanaStreamListener:
    """
    Python implementation of Scala GrafanaStreamListener trait which will receive the events forwarded by manager
    """

    def __init__(self, app_name, influx_writer):
        self.uuid = None
        self._influx_writer = influx_writer
        self._app_name = app_name

    def onQueryStarted(self, event):
        print(f"onQueryStarted:{event}")

    def onQueryProgress(self, event):
        progress = event.progress()
        event_json = progress.prettyJson()
        query_name = progress.name()
        query_id = progress.id()
        batch_id = progress.batchId()
        input_rows_per_sec = progress.inputRowsPerSecond()
        num_input_rows = progress.numInputRows()
        processed_row_per_sec = progress.processedRowsPerSecond()
        timestamp = progress.timestamp()
        #print(f"onQueryProgress:{event_json}")
        grafana_event = QueryProgressEvent(self._app_name, query_name, query_id, batch_id, input_rows_per_sec,
                                           num_input_rows,
                                           processed_row_per_sec,
                                           timestamp)
        print(grafana_event)
        self._influx_writer.write(grafana_event)
        print("write successful")

    def onQueryTerminated(self, event):
        print(f"onQueryTerminated:{event}")

    class Java:
        implements = ["org.dataschool.listener.GrafanaStreamListener"]
