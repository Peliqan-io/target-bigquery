# Copyright (c) 2023 Alex Butler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
"""BigQuery Streaming Insert Sink.
Throughput test: ...slower than all other methods, no test results available.
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import os
from multiprocessing import Process
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from typing import Any, Dict, List, NamedTuple, Optional, Type, Union

import orjson, decimal
from google.api_core.exceptions import GatewayTimeout, NotFound
from google.cloud import _http, bigquery
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from target_bigquery.core import BaseBigQuerySink, BaseWorker, Denormalized, bigquery_client_factory

def default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

class Job(NamedTuple):
    """Job to be processed by a worker."""

    table: bigquery.TableReference
    records: List[Dict[str, Any]]
    attempt: int = 1


class StreamingInsertWorker(BaseWorker):
    """Worker that streams data into BigQuery."""

    def run(self) -> None:
        """Run the worker."""
        # A monkey patch since we can't override the default json encoder...
        _http.json = orjson
        client: bigquery.Client = bigquery_client_factory(self.credentials)
        while True:
            try:
                job: Optional[Job] = self.queue.get(timeout=20.0)
            except Empty:
                break
            if job is None:
                break
            try:
                _ = retry(
                    client.insert_rows_json,
                    retry=retry_if_exception_type(
                        (ConnectionError, TimeoutError, NotFound, GatewayTimeout)
                    ),
                    wait=wait_fixed(1),
                    stop=stop_after_delay(10),
                    reraise=True,
                )(table=job.table, json_rows=job.records)
            except Exception as exc:
                job.attempt += 1
                if job.attempt > 3:
                    # TODO: add a metric for this + a DLQ & wrap exception type
                    self.error_notifier.send((exc, self.serialize_exception(exc)))
                    raise
                else:
                    self.queue.put(job)
            else:
                self.job_notifier.send(True)
                self.log_notifier.send(
                    f"[{self.ext_id}] Inserted {len(job.records)} records into {job.table}"
                )
            finally:
                self.queue.task_done()


class StreamingInsertThreadWorker(StreamingInsertWorker, _Thread):
    pass


class StreamingInsertProcessWorker(StreamingInsertWorker, Process):
    pass


class BigQueryStreamingInsertSink(BaseBigQuerySink):
    MAX_WORKERS = os.cpu_count() * 2
    WORKER_CAPACITY_FACTOR = 10
    WORKER_CREATION_MIN_INTERVAL = 1.0

    @staticmethod
    def worker_cls_factory(
        worker_executor_cls: Type[Process], config: Dict[str, Any]
    ) -> Type[Union[StreamingInsertThreadWorker, StreamingInsertProcessWorker,]]:
        Worker = type("Worker", (StreamingInsertWorker, worker_executor_cls), {})
        return Worker

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(record["data"], default=default).decode("utf-8")
        return record

    @property
    def max_size(self) -> int:
        return min(super().max_size, 500)

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.records_to_drain.append(record)

    def process_batch(self, context: Dict[str, Any]) -> None:
        self.global_queue.put(Job(table=self.table.as_ref(), records=self.records_to_drain.copy()))
        self.increment_jobs_enqueued()
        self.records_to_drain = []


class BigQueryStreamingInsertDenormalizedSink(Denormalized, BigQueryStreamingInsertSink):
    pass
