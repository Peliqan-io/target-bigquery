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
"""BigQuery Storage Write Sink.
Throughput test: 11m 0s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import os
from time import sleep
from multiprocessing import Process
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import orjson, decimal
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types, writer
from google.protobuf import json_format
from proto import Message
from tenacity import retry, stop_after_attempt, wait_fixed

if TYPE_CHECKING:
    from target_bigquery.target import TargetBigQuery

import logging

from target_bigquery.core import BaseBigQuerySink, BaseWorker, Denormalized, storage_client_factory
from target_bigquery.proto_gen import proto_schema_factory_v2

logger = logging.getLogger(__name__)

# Stream specific constant
MAX_IN_FLIGHT = 15
"""Maximum number of concurrent requests per worker be processed by grpc before awaiting."""

Dispatcher = Callable[[types.AppendRowsRequest], writer.AppendRowsFuture]
StreamComponents = Tuple[str, writer.AppendRowsStream, Dispatcher]

def default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

def get_application_stream(client: BigQueryWriteClient, job: "Job") -> StreamComponents:
    """Get an application created stream for the parent. This stream must be finalized and committed."""
    write_stream = types.WriteStream()
    write_stream.type_ = types.WriteStream.Type.PENDING
    write_stream = client.create_write_stream(parent=job.parent, write_stream=write_stream)
    job.template.write_stream = write_stream.name
    append_rows_stream = writer.AppendRowsStream(client, job.template)
    rv = (write_stream.name, append_rows_stream)
    job.stream_notifier.send(rv)
    return *rv, retry(
        append_rows_stream.send,
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
        reraise=True,
    )


def get_default_stream(client: BigQueryWriteClient, job: "Job") -> StreamComponents:
    """Get the default storage write API stream for the parent."""
    job.template.write_stream = BigQueryWriteClient.write_stream_path(
        **BigQueryWriteClient.parse_table_path(job.parent), stream="_default"
    )
    append_rows_stream = writer.AppendRowsStream(client, job.template)
    rv = (job.template.write_stream, append_rows_stream)
    job.stream_notifier.send(rv)
    return *rv, retry(
        append_rows_stream.send,
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
        reraise=True,
    )


def generate_request(
    payload: types.ProtoRows,
    offset: Optional[int] = None,
    path: Optional[str] = None,
) -> types.AppendRowsRequest:
    """Generate a request for the storage write API from a payload."""
    request = types.AppendRowsRequest()
    if offset is not None:
        request.offset = int(offset)
    if path is not None:
        request.write_stream = path
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = payload
    request.proto_rows = proto_data
    return request


def _self_contained_descriptor(descriptor):
    """Build a DescriptorProto with every referenced message type embedded as a
    `nested_type`, so the Storage Write API ProtoSchema is self-contained.

    `proto_gen` generates each RECORD column as a *separate* message that the parent
    references by name. `generate_template` used to copy only the top-level descriptor,
    so the Storage Write API could not resolve nested RECORD messages and the stream
    failed to open — which hangs the target (see z3z1ma/target-bigquery issue #71).
    This embeds the nested message definitions and rewrites the field type references
    to point at them, recursively (handles RECORD, REPEATED RECORD and deep nesting)."""
    from google.protobuf import descriptor_pb2

    def build(desc):
        proto = descriptor_pb2.DescriptorProto()
        desc.CopyToProto(proto)
        embedded: Dict[str, str] = {}  # message full_name -> local nested_type name
        for i, fld in enumerate(desc.fields):
            if fld.message_type is None:
                continue
            full_name = fld.message_type.full_name
            if full_name not in embedded:
                nested = build(fld.message_type)  # recurse first (handles deep nesting)
                nested.name = "record_%d" % (len(embedded) + 1)  # unique within this scope
                proto.nested_type.add().MergeFrom(nested)
                embedded[full_name] = nested.name
            # relative type ref -> resolved to the embedded nested_type in this scope
            proto.field[i].type_name = embedded[full_name]
        return proto

    return build(descriptor)


def generate_template(message: Type[Message]):
    """Generate a template for the storage write API from a proto message class."""
    template, proto_schema, proto_data = (
        types.AppendRowsRequest(),
        types.ProtoSchema(),
        types.AppendRowsRequest.ProtoData(),
    )
    # Embed nested (RECORD) message definitions so the schema is self-contained;
    # otherwise the Storage Write API can't resolve nested types, the stream fails
    # to open, and the target hangs (issue #71).
    proto_schema.proto_descriptor = _self_contained_descriptor(message.DESCRIPTOR)
    proto_data.writer_schema = proto_schema
    template.proto_rows = proto_data
    return template


class Job():
    parent: str
    template: types.AppendRowsRequest
    stream_notifier: Connection
    data: types.ProtoRows
    attempts: int = 1
    
    def __init__(self,
        parent,
        template,
        stream_notifier,
        data):
        """Initialize the worker process."""
        self.parent = parent
        self.template = template
        self.stream_notifier = stream_notifier
        self.data = data

class StorageWriteBatchWorker(BaseWorker):
    """Worker process for the storage write API."""

    def __init__(self, *args, **kwargs):
        """Initialize the worker process."""
        super().__init__(*args, **kwargs)
        self.get_stream_components = get_application_stream
        self.awaiting: List[writer.AppendRowsFuture] = []
        self.cache: Dict[str, StreamComponents] = {}
        self.max_errors_before_recycle = 5
        self.offsets: Dict[str, int] = {}
        self.logger=logger

    def run(self):
        """Run the worker process."""
        client: BigQueryWriteClient = storage_client_factory(self.credentials)
        if os.getenv("TARGET_BIGQUERY_DEBUG", "false").lower() == "true":
            bidi_logger = logging.getLogger("google.api_core.bidi")
            bidi_logger.setLevel(logging.DEBUG)
        while True:
            try:
                job: Optional[Job] = self.queue.get(timeout=30.0)
            except Empty:
                break
            if job is None:
                break
            if job.parent not in self.cache or self.cache[job.parent][1]._closed:
                self.cache[job.parent] = self.get_stream_components(client, job)
                self.offsets[job.parent] = 0
            write_stream, _, dispatch = cast(StreamComponents, self.cache[job.parent])
           
            try:
                kwargs = {}
                if write_stream.endswith("_default"):
                    kwargs["offset"] = None
                    kwargs["path"] = write_stream
                else:
                    kwargs["offset"] = self.offsets[job.parent]
                self.awaiting.append(dispatch(generate_request(job.data, **kwargs)))
                
            except Exception as exc:
                job.attempts += 1
                self.logger.info(f"job.attempts : {job.attempts}")
                self.max_errors_before_recycle -= 1
                if job.attempts > 3:
                    # TODO: add a metric for this + a DLQ & wrap exception type
                    self.error_notifier.send((exc, self.serialize_exception(exc)))
                else:
                    self.queue.put(job)
                # Track errors and recycle the stream if we hit a threshold
                # 1 bad payload 👆 is not indicative of a bad bidi stream as it _could_
                # be a transient error or luck of the draw with the first payload.
                # 5 worker-specific errors is a good threshold to recycle the stream
                # and start fresh. This is an arbitrary number and can be adjusted.
                if self.max_errors_before_recycle == 0:
                    self.wait(drain=True)
                    self.close_cached_streams()
                    raise
            else:
                self.log_notifier.send(
                    f"[{self.ext_id}] Sent {len(job.data.serialized_rows)} rows to {write_stream}"
                    f" with offset {self.offsets[job.parent]}."
                )
                self.offsets[job.parent] += len(job.data.serialized_rows)
                if len(self.awaiting) > MAX_IN_FLIGHT:
                    self.wait()
            finally:
                self.queue.task_done()
        # Wait for all in-flight requests to complete after poison pill
        self.logger.info(f"[{self.ext_id}] : {self.offsets}")
        self.wait(drain=True)
        self.close_cached_streams()
        self.logger.info("Worker process exiting.")
        self.log_notifier.send("Worker process exiting.")

    def close_cached_streams(self) -> None:
        """Close all cached streams."""
        for _, stream, _ in self.cache.values():
            try:
                stream.close()
            except Exception as exc:
                self.error_notifier.send((exc, self.serialize_exception(exc)))

    def wait(self, drain: bool = False) -> None:
        """Wait for in-flight requests to complete."""
        while self.awaiting and ((len(self.awaiting) > MAX_IN_FLIGHT // 2) or drain):
            try:
                self.awaiting.pop(0).result()
            except Exception as exc:
                self.error_notifier.send((exc, self.serialize_exception(exc)))
            finally:
                self.job_notifier.send(True)


class StorageWriteStreamWorker(StorageWriteBatchWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_stream_components = get_default_stream


class StorageWriteThreadStreamWorker(StorageWriteStreamWorker, _Thread):
    pass


class StorageWriteProcessStreamWorker(StorageWriteStreamWorker, Process):
    pass


class StorageWriteThreadBatchWorker(StorageWriteBatchWorker, _Thread):
    pass


class StorageWriteProcessBatchWorker(StorageWriteBatchWorker, Process):
    pass


class BigQueryStorageWriteSink(BaseBigQuerySink):
    MAX_WORKERS = os.cpu_count() * 2
    MAX_JOBS_QUEUED = MAX_WORKERS * 2
    WORKER_CAPACITY_FACTOR = 10
    WORKER_CREATION_MIN_INTERVAL = 1.0

    @staticmethod
    def worker_cls_factory(
        worker_executor_cls: Type[Process], config: Dict[str, Any]
    ) -> Type[
        Union[
            StorageWriteThreadStreamWorker,
            StorageWriteProcessStreamWorker,
            StorageWriteThreadBatchWorker,
            StorageWriteProcessBatchWorker,
        ]
    ]:
        if config.get("options", {}).get("storage_write_batch_mode", False):
            Worker = type("Worker", (StorageWriteBatchWorker, worker_executor_cls), {})
        else:
            Worker = type("Worker", (StorageWriteStreamWorker, worker_executor_cls), {})
        return Worker

    def __init__(
        self,
        target: "TargetBigQuery",
        stream_name: str,
        schema: Dict[str, Any],
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.open_streams: Set[Tuple[str, writer.AppendRowsStream]] = set()
        self.parent = BigQueryWriteClient.table_path(
            self.table.project,
            self.table.dataset,
            self.table.name,
        )
        self.stream_notification, self.stream_notifier = target.pipe_cls(False)
        self.template = generate_template(self.proto_schema)

    @property
    def proto_schema(self) -> Type[Message]:
        if not hasattr(self, "_proto_schema"):
            self._proto_schema = proto_schema_factory_v2(
                self.table.get_resolved_schema(self.apply_transforms)
            )
        return self._proto_schema

    def start_batch(self, context: Dict[str, Any]) -> None:
        self.proto_rows = types.ProtoRows()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(record["data"], default=default).decode("utf-8")
        return record

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.proto_rows.serialized_rows.append(
            json_format.ParseDict(record, self.proto_schema()).SerializeToString()
        )

    def process_batch(self, context: Dict[str, Any]) -> None:
        while self.global_queue.qsize() >= self.MAX_JOBS_QUEUED:
            self.logger.warn(f"Max jobs enqueued reached ({self.MAX_JOBS_QUEUED})")
            sleep(1)
        
        self.global_queue.put(
            Job(
                parent=self.parent,
                template=self.template,
                data=self.proto_rows,
                stream_notifier=self.stream_notifier,
            )
        )
        self.increment_jobs_enqueued()

    def commit_streams(self) -> None:
        while self.stream_notification.poll():
            stream_payload = self.stream_notification.recv()
            self.logger.debug("Stream enqueued %s", stream_payload)
            self.open_streams.add(stream_payload)
        if not self.open_streams:
            return
        self.open_streams = [
            (name, stream) for name, stream in self.open_streams if not name.endswith("_default")
        ]
        if self.open_streams:
            committer = storage_client_factory(self._credentials)
            for name, stream in self.open_streams:
                stream.close()
                committer.finalize_write_stream(name=name)
            write = committer.batch_commit_write_streams(
                types.BatchCommitWriteStreamsRequest(
                    parent=self.parent,
                    write_streams=[name for name, _ in self.open_streams],
                )
            )
            self.logger.info(f"Batch commit time: {write.commit_time}")
            self.logger.info(f"Batch commit errors: {write.stream_errors}")
            self.logger.info(f"Writes to streams: '{self.open_streams}' have been committed.")
        self.open_streams = set()

    def clean_up(self) -> None:
        self.commit_streams()
        super().clean_up()

    def pre_state_hook(self) -> None:
        self.commit_streams()


def _stringify_json_columns(record: Dict[str, Any], fields: List[Any]) -> Dict[str, Any]:
    """Serialize values bound for BigQuery JSON columns to strings, recursively.

    The Storage Write API encodes rows as protobuf, and `proto_gen` maps a BigQuery
    JSON column to a proto STRING field. So a value heading into a JSON column must be
    a JSON *string* before `json_format.ParseDict`, otherwise it raises
    `ParseError: expected string ... got 'dict'`. batch_job (NDJSON) does not need this
    — which is why this lives on the storage_write denormalized sink only."""
    for field in fields:
        value = record.get(field.name)
        if value is None:
            continue
        ftype = field.field_type.upper()
        if ftype == "JSON":
            if field.mode == "REPEATED" and isinstance(value, list):
                record[field.name] = [
                    v if isinstance(v, str) else orjson.dumps(v, default=default).decode("utf-8")
                    for v in value
                ]
            elif not isinstance(value, str):
                record[field.name] = orjson.dumps(value, default=default).decode("utf-8")
        elif ftype == "RECORD" and field.fields:
            items = value if (field.mode == "REPEATED" and isinstance(value, list)) else [value]
            for item in items:
                if isinstance(item, dict):
                    _stringify_json_columns(item, field.fields)
    return record


class BigQueryStorageWriteDenormalizedSink(Denormalized, BigQueryStorageWriteSink):
    def preprocess_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        # Denormalized.preprocess_record (translate_record) runs first, then we serialize
        # JSON-typed values to strings so they can be proto-encoded (see helper docstring).
        record = super().preprocess_record(record, context)
        return _stringify_json_columns(
            record, self.table.get_resolved_schema(self.apply_transforms)
        )
