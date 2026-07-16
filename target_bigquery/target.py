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
"""BigQuery target class."""
import os
import copy
import time
import uuid
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Type, Union

from singer_sdk import Sink
from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_bigquery.batch_job import BigQueryBatchJobDenormalizedSink, BigQueryBatchJobSink
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    BigQueryCredentials,
    ParType,
    bigquery_client_factory,
    transform_column_name,
)
from target_bigquery.gcs_stage import BigQueryGcsStagingDenormalizedSink, BigQueryGcsStagingSink
from target_bigquery.storage_write import (
    BigQueryStorageWriteDenormalizedSink,
    BigQueryStorageWriteSink,
)
from target_bigquery.streaming_insert import (
    BigQueryStreamingInsertDenormalizedSink,
    BigQueryStreamingInsertSink,
)

if TYPE_CHECKING:
    from multiprocessing import Process, Queue
    from multiprocessing.connection import Connection

# Defaults for target worker pool parameters
MAX_WORKERS = 15
"""Maximum number of workers to spawn."""
MAX_JOBS_QUEUED = 30
"""Maximum number of jobs placed in the global queue to avoid memory overload."""
WORKER_CAPACITY_FACTOR = 5
"""Jobs enqueued must exceed the number of active workers times this number."""
WORKER_CREATION_MIN_INTERVAL = 5
"""Minimum time between worker creation attempts."""


class TargetBigQuery(Target):
    """Target for BigQuery."""

    _MAX_RECORD_AGE_IN_MINUTES = 5.0
    
    name = "target-bigquery"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "credentials_path",
            th.StringType,
            description="The path to a gcp credentials json file.",
        ),
        th.Property(
            "credentials_json",
            th.StringType,
            description="A JSON string of your service account JSON file.",
        ),
        th.Property(
            "project",
            th.StringType,
            description="The target GCP project to materialize data into.",
            required=True,
        ),
        th.Property(
            "dataset",
            th.StringType,
            description="The target dataset to materialize data into.",
            required=True,
        ),
        th.Property(
            "location",
            th.StringType,
            description="The target dataset/bucket location to materialize data into.",
            default="US",
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="The maximum number of rows to send in a single batch or commit.",
            default=500,
        ),
        th.Property(
            "fail_fast",
            th.BooleanType,
            description="Fail the entire load job if any row fails to insert.",
            default=True,
        ),
        th.Property(
            "timeout",
            th.IntegerType,
            description="Default timeout for batch_job and gcs_stage derived LoadJobs.",
            default=600,
        ),
        th.Property(
            "denormalized",
            th.BooleanType,
            description=(
                "Determines whether to denormalize the data before writing to BigQuery. A false"
                " value will write data using a fixed JSON column based schema, while a true value"
                " will write data using a dynamic schema derived from the tap."
            ),
            default=False,
        ),
        th.Property(
            "method",
            th.CustomType(
                {
                    "type": "string",
                    "enum": [
                        "storage_write_api",
                        "batch_job",
                        "gcs_stage",
                        "streaming_insert",
                    ],
                }
            ),
            description="The method to use for writing to BigQuery.",
            default="storage_write_api",
            required=True,
        ),
        th.Property(
            "generate_view",
            th.BooleanType,
            description=(
                "Determines whether to generate a view based on the SCHEMA message parsed from the"
                " tap. Only valid if denormalized=false meaning you are using the fixed JSON column"
                " based schema."
            ),
            default=False,
        ),
        th.Property(
            "bucket",
            th.StringType,
            description="The GCS bucket to use for staging data. Only used if method is gcs_stage.",
        ),
        th.Property(
            "partition_granularity",
            th.CustomType(
                {
                    "type": "string",
                    "enum": [
                        "year",
                        "month",
                        "day",
                        "hour",
                    ],
                }
            ),
            default="month",
            description="The granularity of the partitioning strategy. Defaults to month.",
        ),
        th.Property(
            "cluster_on_key_properties",
            th.BooleanType,
            default=False,
            description=(
                "Determines whether to cluster on the key properties from the tap. Defaults to"
                " false. When false, clustering will be based on _sdc_batched_at instead."
            ),
        ),
        th.Property(
            "column_name_transforms",
            th.ObjectType(
                th.Property(
                    "lower",
                    th.BooleanType,
                    default=False,
                    description="Lowercase column names",
                ),
                th.Property(
                    "quote",
                    th.BooleanType,
                    default=False,
                    description="Quote columns during DDL generation",
                ),
                th.Property(
                    "add_underscore_when_invalid",
                    th.BooleanType,
                    default=False,
                    description="Add an underscore when a column starts with a digit",
                ),
                th.Property(
                    "snake_case",
                    th.BooleanType,
                    default=False,
                    description="Convert columns to snake case",
                ),
            ),
            description=(
                "Accepts a JSON object of options with boolean values to enable them. The available"
                " options are `quote` (quote columns in DDL), `lower` (lowercase column names),"
                " `add_underscore_when_invalid` (add underscore if column starts with digit), and"
                " `snake_case` (convert to snake case naming). For fixed schema, this transform"
                " only applies to the generated view if enabled."
            ),
            required=False,
        ),
        th.Property(
            "table_name_prefix",
            th.StringType,
            description="A fixed prefix prepended to all table names (e.g. 'bronze_').",
            required=False,
        ),
        th.Property(
            "options",
            th.ObjectType(
                th.Property(
                    "storage_write_batch_mode",
                    th.BooleanType,
                    default=False,
                    description=(
                        "By default, we use the default stream (Committed mode) in the"
                        " storage_write_api load method which results in streaming records which"
                        " are immediately available and is generally fastest. If this is set to"
                        " true, we will use the application created streams (Committed mode) to"
                        " transactionally batch data on STATE messages and at end of pipe."
                    ),
                ),
                th.Property(
                    "process_pool",
                    th.BooleanType,
                    default=False,
                    description=(
                        "By default we use an autoscaling threadpool to write to BigQuery. If set"
                        " to true, we will use a process pool."
                    ),
                ),
                th.Property(
                    "max_workers",
                    th.IntegerType,
                    required=False,
                    description=(
                        "By default, each sink type has a preconfigured max worker pool limit."
                        " This sets an override for maximum number of workers in the pool."
                    ),
                ),
            ),
            description=(
                "Accepts a JSON object of options with boolean values to enable them. These are"
                " more advanced options that shouldn't need tweaking but are here for flexibility."
            ),
        ),
        th.Property(
            "upsert",
            th.CustomType(
                {
                    "anyOf": [
                        {"type": "boolean"},
                        {"type": "array", "items": {"type": "string"}},
                    ]
                }
            ),
            default=False,
            description=(
                "Determines if we should upsert. Defaults to false. A value of true will write to a"
                " temporary table and then merge into the target table (upsert). This requires the"
                " target table to be unique on the key properties. A value of false will write to"
                " the target table directly (append). A value of an array of strings will evaluate"
                " the strings in order using fnmatch. At the end of the array, the value of the"
                " last match will be used. If not matched, the default value is false (append)."
            ),
        ),
        th.Property(
            "overwrite",
            th.CustomType(
                {
                    "anyOf": [
                        {"type": "boolean"},
                        {"type": "array", "items": {"type": "string"}},
                    ]
                }
            ),
            default=False,
            description=(
                "Determines if the target table should be overwritten on load. Defaults to false. A"
                " value of true will write to a temporary table and then overwrite the target table"
                " inside a transaction (so it is safe). A value of false will write to the target"
                " table directly (append). A value of an array of strings will evaluate the strings"
                " in order using fnmatch. At the end of the array, the value of the last match will"
                " be used. If not matched, the default value is false. This is mutually exclusive"
                " with the `upsert` option. If both are set, `upsert` will take precedence."
            ),
        ),
        th.Property(
            "dedupe_before_upsert",
            th.CustomType(
                {
                    "anyOf": [
                        {"type": "boolean"},
                        {"type": "array", "items": {"type": "string"}},
                    ]
                }
            ),
            default=False,
            description=(
                "This option is only used if `upsert` is enabled for a stream. The selection"
                " criteria for the stream's candidacy is the same as upsert. If the stream is"
                " marked for deduping before upsert, we will create a _session scoped temporary"
                " table during the merge transaction to dedupe the ingested records. This is useful"
                " for streams that are not unique on the key properties during an ingest but are"
                " unique in the source system. Data lake ingestion is often a good example of this"
                " where the same unique record may exist in the lake at different points in time"
                " from different extracts."
            ),
        ),
        th.Property(
            "schema_resolver_version",
            th.IntegerType,
            default=2,
            description=(
                "The version of the schema resolver to use. Defaults to 2. Version 2 uses JSON as a"
                " fallback during denormalization. This only has an effect if denormalized=true"
            ),
            allowed_values=[1, 2],
        ),
        th.Property(
            "checkpoint_row_threshold",
            th.IntegerType,
            default=10_000,
            description=(
                "The number of records to process (across all streams) before triggering a mid-run"
                " checkpoint (MERGE staged rows into the target table and emit a STATE message) at"
                " the next category/stream boundary. Only applies to merge/upsert streams. A value"
                " of 0 checkpoints at every category boundary; a very large value effectively"
                " disables mid-run checkpoints (only end-of-pipe finalizes)."
            ),
        ),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.max_parallelism = 1
        # Buffer for DELETERECORD Singer messages keyed by stream name.
        # Populated in _process_unknown_message; flushed once at end-of-pipe
        # after all sink.clean_up() calls so upserts always land before deletes.
        self._delete_buffer: dict = {}
        (
            self.proc_cls,
            self.pipe_cls,
            self.queue_cls,
            self.par_typ,
        ) = self.get_parallelization_components()
        self.queue = self.queue_cls()
        self.job_notification, self.job_notifier = self.pipe_cls(False)
        self.log_notification, self.log_notifier = self.pipe_cls(False)
        self.error_notification, self.error_notifier = self.pipe_cls(False)
        self._credentials = BigQueryCredentials(
            self.config.get("credentials_path"),
            self.config.get("credentials_json"),
            self.config["project"],
        )

        def worker_factory():
            return self.get_sink_class().worker_cls_factory(
                self.proc_cls,
                self.config,
            )(
                ext_id=uuid.uuid4().hex,
                queue=self.queue,
                credentials=self._credentials,
                job_notifier=self.job_notifier,
                log_notifier=self.log_notifier,
                error_notifier=self.error_notifier,
            )

        self.worker_factory = worker_factory
        self.workers: List[Union[BaseWorker, "Process"]] = []
        self.worker_pings: Dict[str, float] = {}
        self._jobs_enqueued = 0
        self._last_worker_creation = 0.0
        # Mid-run checkpoint trigger (PQ-3547): count records across all
        # streams since the last checkpoint; a STATE message at or past the
        # threshold triggers a non-endofpipe drain_all() (MERGE + emit state).
        self._checkpoint_row_threshold = int(self.config.get("checkpoint_row_threshold", 10_000))
        self._records_since_checkpoint = 0

    def increment_jobs_enqueued(self) -> None:
        """Increment the number of jobs enqueued."""
        self._jobs_enqueued += 1

    # We can expand this to support other parallelization methods in the future.
    # We woulod approach this by adding a new ParType enum and interpreting the
    # the Process, Pipe, and Queue classes as protocols which can be duck-typed.

    def get_parallelization_components(
        self, default=ParType.THREAD
    ) -> Tuple[
        Type["Process"],
        Callable[[bool], Tuple["Connection", "Connection"]],
        Callable[[], "Queue"],
        ParType,
    ]:
        """Get the appropriate Process, Pipe, and Queue classes and the assoc ParTyp enum."""
        use_procs: Optional[bool] = self.config.get("options", {}).get("process_pool")

        if use_procs is None:
            use_procs = default == ParType.PROCESS

        if not use_procs:
            from multiprocessing.dummy import Pipe, Process, Queue

            self.logger.info("Using thread-based parallelism")
            return Process, Pipe, Queue, ParType.THREAD
        else:
            from multiprocessing import Pipe, Process, Queue

            self.logger.info("Using process-based parallelism")
            return Process, Pipe, Queue, ParType.PROCESS

    # Worker management methods, which are used to manage the number of
    # workers in the pool. The ensure_workers method should be called
    # periodically to ensure that the pool is at the correct size, ideally
    # once per batch.

    @property
    def add_worker_predicate(self) -> bool:
        """Predicate determining when it is valid to add a worker to the pool."""
        return (
            self._jobs_enqueued
            > getattr(self.get_sink_class(), "WORKER_CAPACITY_FACTOR", WORKER_CAPACITY_FACTOR)
            * (len(self.workers) + 1)
            and len(self.workers)
            < self.config.get("options", {}).get(
                "max_workers",
                getattr(self.get_sink_class(), "MAX_WORKERS", MAX_WORKERS),
            )
            and time.time() - self._last_worker_creation
            > getattr(
                self.get_sink_class(),
                "WORKER_CREATION_MIN_INTERVAL",
                WORKER_CREATION_MIN_INTERVAL,
            )
        )

    def resize_worker_pool(self) -> None:
        """Right-sizes the worker pool.

        Workers self terminate when they have been idle for a while.
        This method will remove terminated workers and add new workers
        if the add_worker_predicate evaluates to True. It will always
        ensure that there is at least one worker in the pool."""
        workers_to_cull = []
        worker_spawned = False
        for i, worker in enumerate(self.workers):
            if not worker.is_alive():
                workers_to_cull.append(i)
        for i in reversed(workers_to_cull):
            worker = self.workers.pop(i)
            worker.join()  # Wait for the worker to terminate. This should be a no-op.
            self.logger.info("Culling terminated worker %s", worker.ext_id)
        while self.add_worker_predicate or not self.workers:
            worker = self.worker_factory()
            worker.start()
            self.workers.append(worker)
            worker_spawned = True
            self.logger.info("Adding worker %s", worker.ext_id)
            self._last_worker_creation = time.time()
        if worker_spawned:
            ...

    # SDK overrides to inject our worker management logic and sink selection.

    def get_sink_class(self, stream_name: Optional[str] = None) -> Type[BaseBigQuerySink]:
        """Returns the sink class to use for a given stream based on user config."""
        _ = stream_name
        method, denormalized = self.config.get("method", "storage_write_api"), self.config.get(
            "denormalized", False
        )
        if method == "batch_job":
            if denormalized:
                return BigQueryBatchJobDenormalizedSink
            return BigQueryBatchJobSink
        elif method == "streaming_insert":
            if denormalized:
                return BigQueryStreamingInsertDenormalizedSink
            return BigQueryStreamingInsertSink
        elif method == "gcs_stage":
            if denormalized:
                return BigQueryGcsStagingDenormalizedSink
            return BigQueryGcsStagingSink
        elif method == "storage_write_api":
            if denormalized:
                return BigQueryStorageWriteDenormalizedSink
            return BigQueryStorageWriteSink
        raise ValueError(f"Unknown method: {method}")

    def get_sink(
        self,
        stream_name: str,
        *,
        record: Optional[dict] = None,
        schema: Optional[dict] = None,
        key_properties: Optional[List[str]] = None,
    ) -> Sink:
        """Get a sink for a stream. If the sink does not exist, create it. This override skips sink recreation
        on schema change. Meaningful mid stream schema changes are not supported and extremely rare to begin
        with. Most taps provide a static schema at stream init. We handle +90% of cases with this override without
        the undue complexity or overhead of mid-stream schema evolution. If you need to support mid-stream schema
        evolution on a regular basis, you should be using the fixed schema load pattern."""
        _ = record
        if schema is None:
            self._assert_sink_exists(stream_name)
            return self._sinks_active[stream_name]
        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sink(stream_name, schema, key_properties)
        return existing_sink

    # ------------------------------------------------------------------ #
    # delete-sync support                                                  #
    # ------------------------------------------------------------------ #

    supports_delete_sync: bool = True
    """Advertises delete-sync capability to baserow's target-support gate."""

    def _process_unknown_message(self, message_dict: dict) -> None:
        """Intercept DELETERECORD messages; delegate everything else to the SDK.

        DELETERECORD is a Peliqan Singer extension not in the SDK's
        SingerMessageType enum, so the SDK routes it here.  We buffer the
        record keyed by stream; _flush_deletes() drains the buffer at
        end-of-pipe, after all sink.clean_up() upserts have completed.
        """
        if message_dict.get("type") == "DELETERECORD":
            stream = message_dict.get("stream", "")
            record = message_dict.get("record", {})
            self._delete_buffer.setdefault(stream, []).append(record)
            return
        super()._process_unknown_message(message_dict)

    def _process_record_message(self, message_dict: dict) -> None:
        """Process a RECORD message, then tally it toward the checkpoint window."""
        super()._process_record_message(message_dict)
        self._records_since_checkpoint += 1

    def _method_supports_checkpoint(self) -> bool:
        """Whether the configured target method supports the non-destructive
        checkpoint()/MERGE path PQ-3547 relies on. Only `batch_job` and
        `storage_write_api` do; `gcs_stage` and `streaming_insert` keep their
        origin/master mid-run behavior. The default method is
        `storage_write_api`."""
        return self.config.get("method", "storage_write_api") in (
            "batch_job",
            "storage_write_api",
        )

    def _sink_checkpoint_eligible(self, sink: "BaseBigQuerySink") -> bool:
        """Whether an individual active sink may be checkpointed (MERGE) on a
        mid-run drain. Eligible only when the method supports checkpointing AND
        the sink is upserting (`merge_target is not None`) AND it is not an
        overwrite sink (`overwrite_target is None`). An overwrite sink DROPs +
        recreates the real table, and mid-run that would truncate/replace it
        before end-of-pipe -- not this feature's contract.

        Used per sink in drain_all()'s non-endofpipe branch: this is the gate
        that also covers the singer-sdk max-age drain, which calls
        drain_all(is_endofpipe=False) directly and bypasses the
        _process_state_message trigger below."""
        return (
            self._method_supports_checkpoint()
            and getattr(sink, "merge_target", None) is not None
            and getattr(sink, "overwrite_target", None) is None
        )

    def _row_threshold_checkpoint_eligible(self) -> bool:
        """Whether this run is in scope for PQ-3547 row-threshold mid-run
        checkpoints (MERGE + STATE at a category boundary).

        Requires the method to support checkpointing and at least one active
        sink to be actually upserting (`merge_target is not None`). If ANY
        active sink is in overwrite mode the whole run stays out of scope
        (origin/master behavior) -- §5.3 trigger condition "no active sink is
        in overwrite mode". The per-sink drain_all gate
        (_sink_checkpoint_eligible) is finer-grained; this run-level gate only
        decides whether the row-threshold trigger fires at all."""
        if not self._method_supports_checkpoint():
            return False
        sinks = list(self._sinks_active.values())
        if any(getattr(sink, "overwrite_target", None) is not None for sink in sinks):
            return False
        return any(getattr(sink, "merge_target", None) is not None for sink in sinks)

    def _process_state_message(self, message_dict: dict) -> None:
        """Process a STATE message (category/stream boundary), then checkpoint
        mid-run if enough records have accrued since the last checkpoint.

        threshold == 0 means every category boundary checkpoints (counter is
        always >= 0). A very large threshold effectively disables mid-run
        checkpoints (only max-age and end-of-pipe still finalize).
        """
        super()._process_state_message(message_dict)
        if (
            self._checkpoint_row_threshold >= 0
            and self._records_since_checkpoint >= self._checkpoint_row_threshold
            and self._row_threshold_checkpoint_eligible()
        ):
            self.drain_all(is_endofpipe=False)

    def drain_one(self, sink: Sink) -> None:  # type: ignore
        """Drain a sink. Includes a hook to manage the worker pool and notifications."""
        #self.logger.info(f"Jobs queued : {self.queue.qsize()} | Max nb jobs queued : {os.cpu_count() * 4} | Nb workers : {len(self.workers)} | Max nb workers : {os.cpu_count() * 2}")
        self.resize_worker_pool()
        while self.job_notification.poll():
            ext_id = self.job_notification.recv()
            self.worker_pings[ext_id] = time.time()
            self._jobs_enqueued -= 1
        while self.log_notification.poll():
            msg = self.log_notification.recv()
            self.logger.info(msg)
        if self.error_notification.poll():
            e, msg = self.error_notification.recv()
            if self.config.get("fail_fast", True):
                self.logger.error(msg)
                try:
                    # Try to drain if we can. This is a best effort.
                    # TODO: we should consider if draining here is the right thing
                    # to do. It's _possible_ we increment the state message when
                    # data is not actually written. Its _unlikely_ so the upside is
                    # greater than the downside for now but will revisit this.
                    self.logger.error("Draining all sinks and terminating.")
                    self.drain_all(is_endofpipe=True)
                except Exception:
                    self.logger.error("Drain failed.")
                raise RuntimeError(msg) from e
        super().drain_one(sink)

    def drain_all(self, is_endofpipe: bool = False) -> None:  # type: ignore
        """Drain all sinks and write state message. If is_endofpipe, execute clean_up() on all sinks.

        Fail-fast checkpoint finalization (PQ-3547): sinks are finalized
        (checkpoint() mid-run / clean_up() at end-of-pipe) in a plain loop
        with no per-sink exception handling. The first failure propagates
        immediately, before `_flush_deletes()` and before
        `_write_state_message()` run, so a failed checkpoint/clean_up emits
        no STATE for that boundary and the process exits non-zero. Earlier
        sinks that already finalized successfully in the same loop are not
        rolled back -- their data is durable and safe to replay (upsert +
        dedupe), but no new STATE reflects them until a later run succeeds.
        """
        state = copy.deepcopy(self._latest_state)
        sink: BaseBigQuerySink
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        if is_endofpipe:
            for worker in self.workers:
                if worker.is_alive():
                    self.queue.put(None)
            while len(self.workers):
                worker.join()
                worker = self.workers.pop()
            self._raise_pending_worker_error()
            for sink in self._sinks_active.values():
                sink.clean_up()
        else:
            for worker in self.workers:
                worker.join()
            self._raise_pending_worker_error()
            # Per-sink scope gate (PQ-3547 §5.3/§10). This branch also runs on
            # the singer-sdk max-age timer (_handle_max_record_age ->
            # drain_all(is_endofpipe=False)), which bypasses the
            # _process_state_message trigger. Only checkpoint (MERGE) sinks that
            # are genuinely in scope; every out-of-scope sink
            # (gcs_stage/streaming_insert/overwrite) keeps its origin/master
            # pre_state_hook() behavior, so a mid-run drain never MERGEs an
            # empty staging table and advances the bookmark past data that is
            # only durable at clean_up(). Fail-fast: no try/except -- the first
            # checkpoint() error propagates before the counter reset and STATE.
            for sink in self._sinks_active.values():
                if self._sink_checkpoint_eligible(sink):
                    sink.checkpoint()
                else:
                    sink.pre_state_hook()
            self._records_since_checkpoint = 0
        # Apply buffered DELETERECORDs *before* advancing state, so the bookmark
        # never moves past deletes that were not applied. At end-of-pipe this runs
        # after the clean_up() upserts above (an upsert + delete of the same key in
        # one run ends deleted). A real delete failure raises here, so the state
        # write below is skipped and the tap replays the deletes on the next run.
        if self._delete_buffer:
            self._flush_deletes()
        if state:
            self._write_state_message(state)
        self._reset_max_record_age()

    def _raise_pending_worker_error(self) -> None:
        """Surface worker errors that arrived after the last drain_one poll.

        Workers report failures (e.g. a record too large for any AppendRows
        request, or a job that exhausted its retries) via the error notifier,
        which is normally polled in drain_one. Errors raised on the final jobs
        land after that last poll — without this check the target would exit 0
        and write a state message while rows were silently dropped."""
        if self.error_notification.poll() and self.config.get("fail_fast", True):
            e, msg = self.error_notification.recv()
            self.logger.error(msg)
            raise RuntimeError(msg) from e
    def _flush_deletes(self) -> None:
        """Apply buffered DELETERECORDs as one batched, parameterized DELETE per
        table. Storage-mode-aware (FIXED -> JSON_VALUE(data,...), DENORMALIZED ->
        real columns).

        Error policy mirrors target-postgres: a delete that cannot be *applied* is
        a best-effort skip -- a key matching no rows is not an error in BigQuery
        (0 rows affected, no exception), and a missing table/dataset (NotFound,
        e.g. a stream never synced) is logged and skipped. Any other failure
        (connection, credentials, permission, quota, bad SQL) is raised so the
        caller does not advance the bookmark past deletes that never landed."""
        client = bigquery_client_factory(self._credentials)
        denormalized = self.config.get("denormalized", False)
        project, dataset = self.config["project"], self.config["dataset"]
        prefix = self.config.get("table_name_prefix", "")
        transforms = self.config.get("column_name_transforms", {})

        def accessor(col: str) -> str:
            # Compare as text: JSON_VALUE returns STRING; CAST aligns typed columns
            # with the str()-rendered parameter values.
            if denormalized:
                # Denormalized writes store columns under the transformed name
                # (core.SchemaTranslator.translate_record); apply the same
                # transform so the delete predicate targets the real column.
                physical = transform_column_name(col, **{**transforms, "quote": False})
                return f"CAST(`{physical}` AS STRING)"
            return f"JSON_VALUE(data, '$.{col}')"

        # Reuse the write-path batch size (baserow -> 5000, default fallback
        # 10_000). The values ride in a query parameter so query length is not the
        # limit, but chunking keeps each request well under BigQuery's 10 MB cap
        # and emits one DELETE per chunk (DML-quota friendly).
        batch_size = self.config.get("batch_size", 10_000)

        for stream, records in self._delete_buffer.items():
            if not records:
                continue
            name = prefix + stream.lower().replace("-", "_").replace(".", "_")
            escaped = f"`{project}`.`{dataset}`.`{name}`"
            cols = list(records[0].keys())  # stable column order across the batch

            if len(cols) == 1:
                key_expr = accessor(cols[0])
                vals = [str(r[cols[0]]) for r in records]
            else:
                # Composite key: join per-column text with the unit-separator
                # control char (assumed absent from GUID/int/text key values).
                key_expr = "CONCAT(" + ", '\\u001f', ".join(accessor(c) for c in cols) + ")"
                vals = ["\u001f".join(str(r[c]) for c in cols) for r in records]

            sql = f"DELETE FROM {escaped} WHERE {key_expr} IN UNNEST(@vals)"
            try:
                for start in range(0, len(vals), batch_size):
                    chunk = vals[start:start + batch_size]
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ArrayQueryParameter("vals", "STRING", chunk)
                        ]
                    )
                    client.query(sql, job_config=job_config).result()
                self.logger.info(
                    "delete-sync: removed up to %d row(s) from %s", len(vals), name
                )
            except NotFound:
                # Table/dataset absent (e.g. a stream that was never synced). The
                # delete cannot apply; skip best-effort and continue other streams.
                self.logger.warning(
                    "delete-sync: table %s not found; skipping its deletes", name
                )
                continue
            # Any other exception (connection, credentials, permission, quota, bad
            # SQL) propagates: drain_all aborts before writing state, so the
            # bookmark is not advanced and the tap replays these deletes next run.

        self._delete_buffer.clear()
