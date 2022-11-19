import datetime

import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class QueryProgressEvent:
    def __init__(self, app_name, query_name, query_id, batch_id, input_rows_per_sec, num_input_rows,
                 processed_row_per_sec,
                 timestamp):
        self._app_name = app_name
        self._query_name = query_name
        self._query_id = query_id
        self._input_rows_per_sec = input_rows_per_sec
        self._processed_row_per_sec = processed_row_per_sec
        self._timestamp = timestamp
        self._batch_id = batch_id
        self._num_input_rows = num_input_rows

    @property
    def query_name(self):
        return self._query_name

    @property
    def input_rows_per_sec(self):
        return self._input_rows_per_sec

    @property
    def processed_row_per_sec(self):
        return self._processed_row_per_sec

    @property
    def event_time(self):
        return self._event_time

    def convert_to_influx_record(self):
        point = (
            Point(f"{self._app_name}")
            .tag("query_name", {str(self._query_name)})
            .tag("query_id", {str(self._query_id)})
            .field("batch_id", self._batch_id)
            .field("input_rows_per_sec", self._input_rows_per_sec)
            .field("processed_row_per_sec", self._processed_row_per_sec)
            .field("time_stamp", self._timestamp)
            .field("numInputRows", self._num_input_rows)
        )
        return point

    def __str__(self):
        return f"QueryProgressEvent(app_name={self._app_name}," \
               f"query_name={self._query_name}, " \
               f"query_id={self._query_id}, " \
               f"batch_id={self._batch_id}," \
               f" input_rows_per_sec={self._input_rows_per_sec}," \
               f" num_input_rows={self._num_input_rows}," \
               f"processed_row_per_sec={self._processed_row_per_sec}," \
               f"timestamp={self._timestamp}"


class InfluxWriter:
    def __init__(self, org, url, bucket, token):
        self._org = org
        self._url = url
        self._bucket = bucket
        token = token
        client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        self._write_api = client.write_api(write_options=SYNCHRONOUS)

    @property
    def org(self):
        return self._org

    @property
    def url(self):
        return self._url

    @property
    def bucket(self):
        return self._bucket

    @property
    def write_api(self):
        return self._write_api

    def write(self, event):
        record = event.convert_to_influx_record()
        self._write_api.write(bucket=self._bucket, org=self._org, record=record)
