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
from io import BytesIO
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
from google.cloud import bigquery
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types, writer
from google.protobuf import json_format
from proto import Message
from tenacity import retry, stop_after_attempt, wait_fixed

if TYPE_CHECKING:
    from target_bigquery.target import TargetBigQuery

import logging

from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    Compressor,
    Denormalized,
    bigquery_client_factory,
    storage_client_factory,
)
from target_bigquery.proto_gen import proto_schema_factory_v2

logger = logging.getLogger(__name__)

# Stream specific constant
MAX_IN_FLIGHT = 15
"""Maximum number of concurrent requests per worker be processed by grpc before awaiting."""

MAX_REQUEST_BYTES = 19_500_000
"""Byte budget for a single AppendRows request payload.

The Storage Write API hard-caps each AppendRows request at 20 MB. 19.5 decimal MB
stays under the cap whichever way Google means it (20,000,000 or 20,971,520 bytes)
and leaves headroom for the request envelope and, on the first request of a stream,
the writer-schema descriptor merged in from the template. Overridable per pipeline
via `options.max_request_bytes`."""

PER_ROW_OVERHEAD = 8
"""Conservative per-row proto framing cost inside ProtoRows (tag byte + length varint)."""


class RecordTooLargeError(ValueError):
    """A single serialized row exceeds the AppendRows request budget.

    Such a row can never be sent via the Storage Write API, so retrying is
    pointless — the worker reports it straight to the error notifier instead of
    re-enqueueing. (Planned follow-up: divert these rows to a Load Job fallback
    in the sink before they ever reach the queue.)"""


def chunk_proto_rows(rows: types.ProtoRows, limit: int = MAX_REQUEST_BYTES):
    """Split ProtoRows into chunks whose payload stays under the request budget.

    Rows are already serialized, so sizes are exact; order is preserved. Yields
    the original ProtoRows untouched when everything fits in one request."""
    chunk, size = types.ProtoRows(), 0
    for row in rows.serialized_rows:
        row_size = len(row) + PER_ROW_OVERHEAD
        if row_size > limit:
            raise RecordTooLargeError(
                f"A single record serializes to {row_size} bytes, which exceeds the "
                f"AppendRows request budget of {limit} bytes. It cannot be loaded via "
                "the Storage Write API."
            )
        if size + row_size > limit:
            yield chunk
            chunk, size = types.ProtoRows(), 0
        chunk.serialized_rows.append(row)
        size += row_size
    if chunk.serialized_rows:
        yield chunk

Dispatcher = Callable[[types.AppendRowsRequest], writer.AppendRowsFuture]
StreamComponents = Tuple[str, writer.AppendRowsStream, Dispatcher]

STREAM_OPEN_TIMEOUT = 90.0
"""How long to wait for the bidi AppendRows stream to open before failing.

The library default is 600s per open attempt. Combined with the tenacity retry
around send() and the worker's job re-enqueue, a single stuck stream open could
block the target for hours — observed as an intermittent hang with nested RECORD
schemas (issue #71). A healthy open takes well under a second, so 90s is generous;
past that we fail loudly and let the worker recycle the stream on a fresh channel."""


class BoundedOpenAppendRowsStream(writer.AppendRowsStream):
    """AppendRowsStream whose open wait is bounded by STREAM_OPEN_TIMEOUT."""

    def _open(self, initial_request, timeout: float = STREAM_OPEN_TIMEOUT):
        return super()._open(initial_request, timeout=timeout)

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
    append_rows_stream = BoundedOpenAppendRowsStream(client, job.template)
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
    append_rows_stream = BoundedOpenAppendRowsStream(client, job.template)
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

    max_request_bytes: int = MAX_REQUEST_BYTES

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
                # Each AppendRowsStream gets its own client, i.e. its own gRPC
                # channel. Opening a second bidi stream on a channel that already
                # carried one intermittently stalls inside _open() (the issue #71
                # hang with nested RECORD schemas); a fresh channel opens reliably.
                # Stream opens are rare (once per table per worker + recycles), so
                # the extra client is cheap.
                client: BigQueryWriteClient = storage_client_factory(self.credentials)
                self.cache[job.parent] = self.get_stream_components(client, job)
                self.offsets[job.parent] = 0
            write_stream, _, dispatch = cast(StreamComponents, self.cache[job.parent])

            try:
                kwargs = {"offset": None, "path": None}
                if write_stream.endswith("_default"):
                    kwargs["path"] = write_stream
                # Split the batch so every AppendRows request stays under the API's
                # 20 MB cap. Offsets advance per chunk (application streams need the
                # exact running offset); backpressure applies inside the loop so a
                # multi-chunk job cannot flood the in-flight window.
                # logger (not log_notifier): notifier messages are only drained during
                # drain_one, so end-of-pipe activity would be invisible in the logs.
                chunks = list(chunk_proto_rows(job.data, self.max_request_bytes))
                if len(chunks) > 1:
                    total_bytes = sum(len(r) for r in job.data.serialized_rows)
                    self.logger.info(
                        f"[{self.ext_id}] Batch of {len(job.data.serialized_rows)} rows"
                        f" ({total_bytes:,} bytes) for {job.parent} exceeds"
                        f" max_request_bytes={self.max_request_bytes:,};"
                        f" splitting into {len(chunks)} AppendRows requests."
                    )
                for idx, chunk in enumerate(chunks, 1):
                    if not write_stream.endswith("_default"):
                        kwargs["offset"] = self.offsets[job.parent]
                    self.awaiting.append(dispatch(generate_request(chunk, **kwargs)))
                    nrows = len(chunk.serialized_rows)
                    chunk_bytes = sum(len(r) for r in chunk.serialized_rows)
                    self.logger.info(
                        f"[{self.ext_id}] Sent request {idx}/{len(chunks)}:"
                        f" {nrows} rows ({chunk_bytes:,} bytes) to {write_stream}"
                        f" with offset {self.offsets[job.parent]}."
                    )
                    self.offsets[job.parent] += nrows
                    if len(self.awaiting) > MAX_IN_FLIGHT:
                        self.wait()
            except RecordTooLargeError as exc:
                # Deterministic failure — retrying cannot help, report immediately.
                self.logger.error(f"[{self.ext_id}] {exc}")
                self.error_notifier.send((exc, self.serialize_exception(exc)))
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
        Worker.max_request_bytes = int(
            config.get("options", {}).get("max_request_bytes", MAX_REQUEST_BYTES)
        )
        return Worker

    def __init__(
        self,
        target: "TargetBigQuery",
        stream_name: str,
        schema: Dict[str, Any],
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        # Queue-depth cap for the process_batch backpressure loop. Each queued Job
        # holds a full batch of serialized rows in memory, so the Peliqan runtime
        # profile (1 worker / 20k-row batches) sets options.max_jobs_queued=1 to
        # bound memory to roughly one in-flight batch + one queued batch.
        self.MAX_JOBS_QUEUED = int(
            self.config.get("options", {}).get("max_jobs_queued", self.MAX_JOBS_QUEUED)
        )
        # Load Job fallback for records too large for any AppendRows request:
        # detected in process_record, buffered as NDJSON, flushed one Load Job
        # per batch into the same table. Rare by definition, so the load-job
        # quota cost is negligible.
        self.max_request_bytes = int(
            self.config.get("options", {}).get("max_request_bytes", MAX_REQUEST_BYTES)
        )
        self._fallback_buffer: Optional[Compressor] = None
        self._fallback_rows = 0
        self._fallback_jobs: List[bigquery.LoadJob] = []
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
        data = json_format.ParseDict(record, self.proto_schema()).SerializeToString()
        if len(data) + PER_ROW_OVERHEAD > self.max_request_bytes:
            # Too large for any AppendRows request — divert to the Load Job fallback
            # instead of poisoning the batch (the worker would reject the whole job).
            self._buffer_oversized_record(record, len(data))
            return
        self.proto_rows.serialized_rows.append(data)

    @property
    def fallback_job_config(self) -> Dict[str, Any]:
        """LoadJobConfig kwargs for the oversized-record fallback (mirrors batch_job)."""
        return {
            "schema": self.table.get_resolved_schema(),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
        }

    def _buffer_oversized_record(self, record: Dict[str, Any], size: int) -> None:
        if self._fallback_buffer is None:
            self._fallback_buffer = Compressor()
        # The record was stringified for proto encoding; a Load Job wants raw JSON
        # values in JSON columns, so reverse it (only these rare rows pay for this).
        row = _destringify_json_columns(
            record, self.table.get_resolved_schema(self.apply_transforms)
        )
        self._fallback_buffer.write(
            orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE, default=default)
        )
        self._fallback_rows += 1
        self.logger.info(
            f"Record of {size:,} bytes exceeds max_request_bytes"
            f"={self.max_request_bytes:,}; diverting to Load Job fallback"
            f" for {self.table.as_ref()}."
        )

    def _flush_fallback_buffer(self) -> None:
        if self._fallback_buffer is None:
            return
        self._fallback_buffer.close()
        client = bigquery_client_factory(self._credentials)
        self._fallback_jobs.append(
            client.load_table_from_file(
                BytesIO(self._fallback_buffer.getvalue()),
                self.table.as_ref(),
                num_retries=3,
                job_config=bigquery.LoadJobConfig(**self.fallback_job_config),
            )
        )
        self.logger.info(
            f"Submitted Load Job fallback with {self._fallback_rows} oversized"
            f" record(s) for {self.table.as_ref()}."
        )
        self._fallback_buffer = None
        self._fallback_rows = 0

    def _wait_for_fallback_jobs(self) -> None:
        """Block until fallback Load Jobs finish — called before state is written."""
        self._flush_fallback_buffer()
        while self._fallback_jobs:
            job = self._fallback_jobs.pop()
            try:
                job.result()
            except Exception:
                self.logger.error(
                    f"Load Job fallback failed for {self.table.as_ref()}: {job.errors}"
                )
                raise

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
        # Upload any oversized rows collected during this batch (non-blocking:
        # completion is awaited in pre_state_hook/clean_up).
        self._flush_fallback_buffer()

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
        self._wait_for_fallback_jobs()
        super().clean_up()

    def pre_state_hook(self) -> None:
        self.commit_streams()
        self._wait_for_fallback_jobs()


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


def _destringify_json_columns(record: Dict[str, Any], fields: List[Any]) -> Dict[str, Any]:
    """Inverse of _stringify_json_columns, for the Load Job fallback path.

    NDJSON Load Jobs expect raw JSON values in JSON columns; the record reaching
    process_record has them serialized to strings for proto encoding. A string
    that fails to parse is left untouched (it was a genuine string value)."""
    for field in fields:
        value = record.get(field.name)
        if value is None:
            continue
        ftype = field.field_type.upper()
        if ftype == "JSON":
            if field.mode == "REPEATED" and isinstance(value, list):
                record[field.name] = [_maybe_json_loads(v) for v in value]
            else:
                record[field.name] = _maybe_json_loads(value)
        elif ftype == "RECORD" and field.fields:
            items = value if (field.mode == "REPEATED" and isinstance(value, list)) else [value]
            for item in items:
                if isinstance(item, dict):
                    _destringify_json_columns(item, field.fields)
    return record


def _maybe_json_loads(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return orjson.loads(value)
        except orjson.JSONDecodeError:
            return value
    return value


class BigQueryStorageWriteDenormalizedSink(Denormalized, BigQueryStorageWriteSink):
    @property
    def fallback_job_config(self) -> Dict[str, Any]:
        # Mirrors BigQueryBatchJobDenormalizedSink.job_config so fallback rows get
        # the same load semantics as the batch_job path.
        return {
            "schema": self.table.get_resolved_schema(),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
            "schema_update_options": [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
            "ignore_unknown_values": True,
        }

    def preprocess_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        # Denormalized.preprocess_record (translate_record) runs first, then we serialize
        # JSON-typed values to strings so they can be proto-encoded (see helper docstring).
        record = super().preprocess_record(record, context)
        return _stringify_json_columns(
            record, self.table.get_resolved_schema(self.apply_transforms)
        )
