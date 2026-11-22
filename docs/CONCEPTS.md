# Concepts guide

Plain-language explanations of the main ideas in this project. Each section has the same layout:
**what it is**, **how it works**, **where it is in this repo**, **trade-offs**, and **questions you may be asked**.

Contents: [Schema Registry](#1-schema-registry) · [Compacted topics](#2-compacted-topics) · [Dead-letter queues](#3-dead-letter-queues-dlqs) · [Manual offset commits](#4-manual-offset-commits) · [Stream-table join](#5-stream-table-join) · [Kafka Connect JDBC sink](#6-kafka-connect-jdbc-sink) · [bytewax stateful job](#7-bytewax-stateful-job) · [Quick glossary](#quick-glossary)

---

## 1. Schema Registry

### What it is
A small HTTP service that stores the **schemas** (the exact field names and types) of the messages on your topics, and checks that any change to a schema is safe. Kafka itself only moves bytes. It has no idea what is inside a message. Schema Registry is how producers and consumers agree on what the bytes mean.

### How it works
1. A producer wants to send a `CandidateScore`. It asks the registry for the schema's numeric **ID** (for example `5`).
2. The message is written as: `[magic byte 0][4-byte schema ID][Avro-encoded data]`. The field names are not repeated in every message, so messages are small.
3. A consumer reads the 4-byte ID, fetches that schema from the registry (once, then caches it), and decodes the bytes.
4. Schemas are stored under a **subject**. By default the subject is `<topic>-value`, for example `candidate-scores-value`. Each change to a schema creates a new **version**.
5. When someone registers a new version, the registry checks it against the **compatibility mode** and rejects it if it would break readers.

Compatibility modes you should know:

| Mode | Meaning | Allowed change (example) | Upgrade first |
|---|---|---|---|
| `BACKWARD` (default) | New schema can read data written with the old one | Add a field **with a default**, delete a field | Consumers |
| `FORWARD` | Old schema can read data written with the new one | Add a field, delete a field that had a default | Producers |
| `FULL` | Both | Add or delete only fields with defaults | Any order |
| `*_TRANSITIVE` | Checked against **all** older versions, not only the previous one | | |
| `NONE` | No check | Anything | Not recommended |

### Where it is in this repo
- Schemas: [`schemas/*.avsc`](../schemas). Every field added after the first version has a `"default"`, so the change is `BACKWARD` compatible.
- Registration: [`infra/init/init.py`](../infra/init/init.py) registers them and sets the compatibility mode when the stack starts.
- Producers use `auto.register.schemas=False` and `use.latest.version=True` ([`common/kafka.py`](../src/recruiting/common/kafka.py)). A service cannot change a contract by accident. Only the init step can.
- [`tests/test_contracts.py`](../tests/test_contracts.py) checks that each Avro schema and its pydantic model have the same fields.
- You can see them in Kafka UI → Schema Registry, or `curl localhost:8081/subjects`.

### Trade-offs
- Adds a service that must be available. Clients cache schemas, so a short outage mostly affects new schemas.
- Evolving schemas needs discipline (defaults, no renames). A rename is a delete plus an add.
- Message keys here are plain strings, so only values have a schema. Production systems often use Avro keys too.

### Questions you may be asked
- **Why Avro and Schema Registry instead of JSON?** Smaller messages, an enforced contract, and safe evolution. With JSON, a producer can rename a field and silently break every consumer.
- **What is in the first 5 bytes of a message?** A magic byte (0) and the 4-byte schema ID.
- **What does BACKWARD compatible mean and who upgrades first?** New schema reads old data. Consumers upgrade first, because they must be able to read both old and new messages.
- **How do you rename a field safely?** Add the new field with a default, migrate producers and consumers, then remove the old field in a later version. (Avro aliases can also help.)
- **Where do schemas get registered in production?** In CI/CD, not by application code at runtime.

---

## 2. Compacted topics

### What it is
A topic with `cleanup.policy=compact`. Instead of deleting old messages by age or size, Kafka keeps **at least the latest message for every key** and removes older messages with the same key. The topic behaves like a table of "latest value per key", stored as a log.

### How it works
- A background **log cleaner** rewrites old (closed) log segments and drops records that have a newer record with the same key. The **active** segment is never compacted. This is why compaction does not appear instantly.
- Offsets do not change. A record that survives keeps its original offset, so offsets have gaps after compaction.
- Order is preserved.
- Deleting a key: send a record with the key and a **null value** (a **tombstone**). Kafka keeps the tombstone for `delete.retention.ms`, so slow consumers see the delete, then removes it.
- Compaction is a "**at least** the last value" guarantee. A consumer reading the head of the log can still see older values that have not been cleaned yet. A consumer must always apply records in order and let the newest one win.
- Settings that control it: `min.cleanable.dirty.ratio` (how much of the log can be uncleaned before cleaning starts), `segment.ms` / `segment.bytes` (when the active segment rolls and becomes eligible), `delete.retention.ms`.

### Where it is in this repo
- `job-requisitions` (key `job_id`) and `job-stats` (key `job_id`), configured in [`infra/topics.json`](../infra/topics.json).
- `make seed` publishes 3 extra edits of `JOB-1001`. After the segment rolls (60 seconds here) and another record arrives, `make inspect TOPIC=job-requisitions` shows only the latest version of each job. This was verified in the build.
- Demo values (`segment.ms=60000`, `min.cleanable.dirty.ratio=0.01`) are chosen to make compaction visible. Defaults are much lazier. Production uses defaults.
- Closing a job is a normal record with `status=CLOSED`, **not** a tombstone. A tombstone would erase the key, and a consumer could not tell "closed" from "never existed".
- Kafka itself uses compacted topics for `__consumer_offsets`, Connect's config topics, and Schema Registry's `_schemas`.

### Trade-offs
- You lose history. If you need an audit trail of every edit, do not compact (or keep a separate event log).
- A new consumer can rebuild the whole table by reading from offset 0, which is the main benefit. It makes the topic a durable, replayable table.
- Compaction is eventual and costs disk I/O.

### Questions you may be asked
- **When would you use a compacted topic?** Reference data, changelogs for state stores, latest-state-per-entity, configuration.
- **Does compaction guarantee one record per key?** No. It guarantees at least the latest. Older ones may still be present until cleaning runs.
- **Why does my compacted topic still show old records?** The active segment is not cleaned, or the dirty ratio has not been reached.
- **How do you delete a key?** Produce a tombstone (null value).
- **Compact vs delete?** Delete removes by time or size. Compact removes by key. `compact,delete` does both.

---

## 3. Dead-letter queues (DLQs)

### What it is
A separate topic that receives messages a consumer could not process, so one bad message does not block everything behind it. The name comes from messaging systems where undeliverable mail goes to a "dead letter" office.

### How it works
- A **poison pill** is a message that fails every time (invalid bytes, empty resume, a bug triggered by that data). If the consumer keeps retrying it, the partition is stuck, because Kafka consumers read in order.
- The pattern: try (with a bounded number of retries), and if it still fails, publish the original payload plus **why it failed** to the DLQ, then **commit the offset** and move on.
- A good DLQ record includes: the original payload, the error type and message, the stack trace, the source topic / partition / offset, the attempt count, and a timestamp.
- Someone must watch it. A DLQ nobody reads is a data-loss bug with extra steps. The usual workflow is alert, triage, fix the cause, **replay** the messages to the original topic.
- Kafka consumers have no built-in DLQ. You write it yourself. Kafka Connect does have one (`errors.deadletterqueue.topic.name`).
- Related patterns: **retry topics** (`orders-retry-1m`, `-10m`) for transient errors with delays, then a final DLQ.

### Where it is in this repo
- `parser-dlq`: resumes shorter than 50 characters, or the LLM failing twice, or undecodable bytes. See [`parser/graph.py`](../src/recruiting/services/parser/graph.py) and [`parser/service.py`](../src/recruiting/services/parser/service.py).
- `matching-dlq`: unknown job (`JOB_NOT_FOUND`) or the LLM failing twice.
- `connect-dlq`: records Kafka Connect cannot write to Postgres.
- Both application DLQs share the [`DeadLetter` schema](../schemas/dead_letter.avsc) and are sunk into the Postgres `dead_letters` table, so the dashboard can show them.
- The agents record failures as graph state instead of raising, so the Kafka layer can publish to the DLQ and still commit.
- Metric `recruiting_dlq_messages_total{service,reason}` and the alert `MessagesInDeadLetterQueue`.
- A closed job is **not** a DLQ case. That is a business decision, so the message is discarded and counted.

### Trade-offs
- **Ordering is broken** for the skipped message. For strict per-key ordering, you may need to halt that key instead.
- The DLQ can hold sensitive data (`parser-dlq` contains resumes). Restrict access and set retention.
- Distinguish **transient** errors (retry, then DLQ) from **permanent** ones (DLQ immediately, as for an empty resume).
- Not having a replay tool is a gap here (listed in the README).

### Questions you may be asked
- **What is a poison pill and how do you handle it?** A message that always fails. Bounded retries, then DLQ, then commit.
- **What do you put in a DLQ record?** Payload, error, trace, origin coordinates, attempts, time.
- **What stops a DLQ from becoming a graveyard?** Alerts, ownership, retention, replay tooling.
- **When would you NOT use a DLQ?** When ordering is critical and skipping is worse than stopping, or when a failure means a bug that will affect every message (fix the bug instead).

---

## 4. Manual offset commits

### What it is
An **offset** is a message's position in a partition. A consumer group stores "the next offset I will read" per partition in the internal topic `__consumer_offsets`. That stored value is the **committed offset**. After a restart or rebalance, the consumer resumes from it. **Manual commit** means your code decides when to commit, instead of a timer doing it for you.

### How it works
- **Auto commit** (`enable.auto.commit=true`) commits on a timer (default 5 s) in the background. If the consumer crashes after the commit but before it finished the work, the message is **lost** (at-most-once in effect). If the work finishes but the commit has not happened, you reprocess (at-least-once).
- **Manual commit after processing** gives **at-least-once**: you only advance the offset once the result is safe. A crash re-delivers the message, so processing must be **idempotent**.
- **Commit before processing** gives at-most-once. It is rarely what you want.
- **Exactly-once** needs Kafka transactions: produce the output and commit the consumed offset in one atomic transaction (`send_offsets_to_transaction`). It only covers effects inside Kafka.
- Sync commit blocks until acknowledged and is safe. Async is faster but a failure can be missed. Committing per message costs a round trip, so high-throughput systems commit in batches.
- Related settings: `max.poll.interval.ms` (if you take longer than this between polls, the consumer is kicked out and a rebalance happens, which matters for slow LLM calls), `auto.offset.reset` (where to start when there is no committed offset).
- The committed offset is "last processed + 1". The client library handles the +1 when you commit a message.

### Where it is in this repo
- [`common/kafka.py`](../src/recruiting/common/kafka.py): `enable.auto.commit=False`, `commit(msg)` is synchronous. Producer uses `acks=all` and idempotence.
- [`common/runner.py`](../src/recruiting/common/runner.py): the loop is poll → decode → handle → commit. The producer call waits for the broker acknowledgement (`flush`), then commit. If producing fails (`DeliveryError`), the offset is **not** committed and the consumer **rewinds** (`seek`) so the same message is retried.
- Success **and** DLQ routing both commit, so a bad message cannot block the partition.
- `max.poll.interval.ms=300000` because LLM calls are slow.
- Because delivery is at-least-once, the sinks are idempotent: Postgres **upserts** on `application_id`, and the bytewax aggregate stores the latest score per application. Re-running the whole pipeline over the same data with a real model produced exactly 10 rows for 10 applications.

### Trade-offs
- At-least-once means you must design for duplicates.
- Per-message sync commit is simple and safe but slow. Batching is faster and means more reprocessing after a crash.
- Exactly-once would add transaction overhead and does not cover non-Kafka effects like a Postgres write or an LLM call.

### Questions you may be asked
- **Why disable auto commit?** To commit only after the work is durable, avoiding lost messages.
- **At-most-once vs at-least-once vs exactly-once?** Commit before work / after work / transactional produce plus offset commit.
- **What happens if the consumer crashes after producing but before committing?** The message is delivered again, so the output is duplicated unless the downstream is idempotent.
- **What is a rebalance and why does a slow handler cause one?** Partitions are reassigned within the group. Exceeding `max.poll.interval.ms` makes the broker think the consumer died.
- **What does the committed offset point to?** The next message to read.

---

## 5. Stream-table join

### What it is
Combining two kinds of data:
- A **stream** is an unbounded sequence of events (every resume that arrives).
- A **table** is the latest value per key (the current definition of each job).

A stream-table join enriches each stream event with the table's current value for the same key. It is how you add reference data to events: "for each resume, look up its job and compare."

### How it works
- The table is built by reading a **compacted topic** from the start and keeping the latest value per key (in memory, or in RocksDB for big tables). It keeps listening, so it follows updates.
- For each stream event, look up the key in the table and emit the combined result.
- This is a **processing-time** join: the event sees the table as it is *when it is processed*, not as it was when the event happened. That is a stream-table join, as opposed to a stream-stream join (matches events within a time window).
- **Co-partitioning:** in Kafka Streams, if each instance holds only its share of the table, the stream and the table must have the same key, the same number of partitions and the same partitioner, so the same key lands on the same instance. The alternative is a **global table**, where every instance loads the whole table. No co-partitioning is needed, but each instance needs the memory for the whole table.
- The two topics are independent, so there is a **race**: a resume can arrive before its job. The consumer needs a policy: wait, retry later, or send it to a DLQ.
- Another subtlety: when a consumer starts, it must finish loading the table **before** it processes the stream, otherwise early events are joined against an incomplete table.

### Where it is in this repo
- [`matching/state_store.py`](../src/recruiting/services/matching/state_store.py): the table. It reads every partition of `job-requisitions` from offset 0, using `assign()` without a consumer group (so a restart always rebuilds the full table), and keeps following. `start()` blocks until each partition has reached its end (`PARTITION_EOF`).
- [`matching/graph.py`](../src/recruiting/services/matching/graph.py): node `fetch_job_context` is the join. A `CLOSED` job means discard. A missing job waits up to 5 seconds and then goes to `matching-dlq` as `JOB_NOT_FOUND`.
- This is the **global-table** style: every instance loads all jobs.
- Tested: updating a job changes the next score, and a closed job produces no output (`tests/test_matching_graph.py`, plus the live run).

### Trade-offs
- Memory per instance grows with the number of jobs. Fine for thousands of jobs, not for hundreds of millions.
- Processing-time semantics: a resume scored just before a job edit uses the old job. Event-time joins need more machinery (Flink, Kafka Streams with timestamps).
- Hand-rolled here to show the idea. In production, use Kafka Streams (`KTable` / `GlobalKTable`), Flink, or ksqlDB, which add RocksDB state, changelog-backed recovery and fault tolerance.

### Questions you may be asked
- **Stream vs table?** Events vs latest state. A table is a compacted stream viewed as state (the stream-table duality).
- **What is co-partitioning and when do you need it?** Same key, same partition count, same partitioner. Needed when a table is partitioned across instances. Not needed for a global table.
- **How do you handle the event arriving before the reference data?** Wait with a timeout, buffer and retry, retry topic, or DLQ.
- **KTable vs GlobalKTable?** Partitioned across instances (needs co-partitioning) vs fully replicated to each instance.
- **Why must the table finish loading before processing the stream?** Otherwise joins run against partial data and give wrong results.

---

## 6. Kafka Connect JDBC sink

### What it is
**Kafka Connect** is a framework that moves data between Kafka and other systems with configuration instead of code. **Source** connectors read from an external system into Kafka. **Sink** connectors write from Kafka into an external system. The **JDBC sink connector** writes records to a relational database (here Postgres).

### How it works
- Vocabulary: a **worker** is a Connect process. A **connector** is a configured job. A connector splits into **tasks** (the parallel units, at most one per topic partition for a sink). **Converters** translate between bytes in Kafka and Connect's internal data format (here `AvroConverter` plus Schema Registry). **SMTs** (single message transforms) make small per-record changes.
- **Distributed mode** stores connector config, offsets and status in Kafka topics (`_connect-configs`, `_connect-offsets`, `_connect-status`). Workers can be added for scale and failover. You manage connectors over a REST API (`:8083`).
- For a sink, Connect consumes the topic as a consumer group and tracks offsets for you.
- The JDBC sink needs a **schema** for each record, because it maps fields to columns. That is why Avro with Schema Registry works well here.
- Key settings: `insert.mode` (`insert`, `upsert`, `update`), `pk.mode` (`record_key`, `record_value`, `kafka`) and `pk.fields`, `auto.create` / `auto.evolve` (let the connector create or alter tables), `table.name.format`.
- **Upsert** makes writes idempotent: a replay or duplicate overwrites the same row. This pairs with at-least-once delivery.
- Error handling: `errors.tolerance=all` plus `errors.deadletterqueue.topic.name` sends bad records to a topic instead of stopping the connector.

### Where it is in this repo
- Connector configs: [`infra/connect/connectors/`](../infra/connect/connectors). Four sinks: scores, dead letters, job stats, job requisitions.
- Image: [`infra/connect/Dockerfile`](../infra/connect/Dockerfile) installs the JDBC plugin with `confluent-hub`. [`register.sh`](../infra/connect/register.sh) registers connectors with an idempotent `PUT`.
- Tables are created up front in [`infra/postgres/init.sql`](../infra/postgres/init.sql) (`auto.create=false`), so the schema is reviewed instead of guessed. The Avro array `suggested_interview_questions` becomes a Postgres `TEXT[]`, and Avro enums become text.
- `sink-dead-letters` reads two topics and uses `pk.mode=kafka`, so the primary key is topic, partition and offset.
- The dashboard reads Postgres. It never writes, because Connect is the only writer.

### Trade-offs
- Less code and built-in offset management, scaling and error handling, compared with a hand-written consumer. The cost is a JVM service, plugin management, and less control over custom logic.
- The sink is at-least-once. Make the target idempotent (upsert).
- `auto.create` and `auto.evolve` are convenient and risky in production, because the connector decides your table design.
- For capturing changes **from** a database into Kafka, the usual tool is Debezium (a CDC source connector), not the JDBC source.

### Questions you may be asked
- **Why use Connect instead of writing a consumer?** Scaling, offset handling, retries, DLQ, and a catalogue of connectors, with no custom code to maintain.
- **What is the difference between a connector and a task?** A connector is the job config. Tasks do the work in parallel.
- **How do you get exactly-once into a database?** Idempotent writes (upsert on a key). Connect gives at-least-once.
- **What happens to a record that violates a constraint?** With `errors.tolerance=none` the task fails. With `all` and a DLQ it is routed to the DLQ topic.
- **Why does the sink need a schema?** It needs field names and types to build the SQL.

---

## 7. bytewax stateful job

### What it is
**bytewax** is a Python stream-processing framework. You write your logic in Python, and the engine underneath is written in Rust (it builds on the Timely Dataflow engine). No JVM and no cluster are needed to start. A **stateful** job remembers something between events, such as a running count or average.

### How it works
- You build a **dataflow**: a graph of **operators** (`map`, `filter`, `key_on`, `stateful_map`, ...). Data flows through as a **stream**.
- **Keyed state:** after `key_on`, every event has a key. `stateful_map` keeps one piece of state **per key** and calls your function with `(state, event)`. The framework makes sure all events for a key reach the same worker, so state is consistent without locks.
- **Workers** run the dataflow in parallel. Keys are divided between them.
- **Recovery:** with recovery enabled, bytewax snapshots state, so a restart resumes from the last snapshot and the matching input offset, instead of recomputing from the beginning. Without it, state lives in memory and is lost on restart.
- **Windows:** operators for time-based aggregation (tumbling, sliding, session) with event-time handling.
- Connectors: Kafka input and output exist, and they work with raw bytes. Avro (de)serialization is your code.

### Where it is in this repo
- [`analytics/flow.py`](../src/recruiting/services/analytics/flow.py): read `candidate-scores` → decode Avro → `key_on(job_id)` → `stateful_map` → encode Avro → write `job-stats`.
- [`analytics/aggregate.py`](../src/recruiting/services/analytics/aggregate.py): the pure function that updates state. The state is a dict of the **latest score per application**, so a duplicate delivery or a re-score does not double count (tested in `tests/test_analytics.py`).
- It reads from the beginning of the topic on start (`starting_offset=-2`), rebuilding its state from the log. This works at this size. The output goes to the compacted `job-stats` topic, so consumers always see the latest value per job, and Connect writes it to Postgres.
- Run it with `make analytics`, or with several workers: `python -m bytewax.run recruiting.services.analytics.flow:flow -w 2`.

### Trade-offs
- Python is easy to use and slower than a JVM engine for very heavy workloads. Fine for this scale.
- Smaller ecosystem than Flink or Kafka Streams.
- Recovery is not enabled here. For real use, turn it on (`PROD:` note in the file) or the job replays everything on restart.
- Output is at-least-once, like the rest of the pipeline. Idempotent aggregation is what keeps results correct.

### Questions you may be asked
- **What is keyed state and why does it matter?** State partitioned by key, so a key's events and state are on one worker and can be updated without locking.
- **Why is the aggregate idempotent here?** It stores the latest value per application instead of incrementing counters.
- **What happens on restart without recovery?** The state is empty and rebuilt by replaying the input.
- **bytewax vs Kafka Streams vs Flink?** Python and a Rust engine, vs a JVM library that runs inside your app, vs a JVM cluster framework with strong event-time and exactly-once support.
- **Why use a dataflow engine for this and not for the LLM agents?** Per-key rolling aggregation is a natural keyed-state problem. The agents are per-message LLM calls, where it adds little.

---

## Quick glossary

| Term | Meaning |
|---|---|
| **KRaft** | Kafka's built-in consensus (Raft) for metadata, replacing Zookeeper. One node can be broker and controller, as in this compose file. |
| **Broker / topic / partition** | A server / a named log / one ordered slice of a topic. Ordering is only guaranteed within a partition. |
| **Key and partitioning** | The key decides the partition (hash of key mod partition count). Same key, same partition, so same-key order is kept. Changing the partition count remaps keys. |
| **Consumer group** | Consumers sharing a group id split a topic's partitions. Each partition goes to one member. Max parallelism = partition count. |
| **Consumer lag** | Latest offset minus committed offset. The main health signal for a consumer. |
| **Idempotent producer** | `enable.idempotence=true`: the broker discards duplicate retries from the same producer. |
| **`acks=all`** | The write is acknowledged only after all in-sync replicas have it. |
| **Retention** | How long (or how large) a `delete` topic keeps data. |
| **Delivery semantics** | At-most-once, at-least-once, exactly-once. This project is at-least-once with idempotent sinks. |
| **Avro** | Compact binary format with a schema. Supports defaults and evolution rules. |
| **Poison pill** | A message that fails every time it is processed. |
| **Tombstone** | A record with a null value, used to delete a key from a compacted topic. |
| **LangGraph** | Library for building an LLM workflow as a graph of nodes with shared state, and conditional edges. |
| **Structured output** | Making an LLM return data that matches a schema (here a pydantic model), then validating it. |
| **Prompt injection** | Text inside untrusted input that tries to instruct the model. Defences here: treat input as data in the prompt, validate outputs in code, and check it in the eval. |
| **LLM-as-judge** | Using a (stronger) model to grade another model's output against criteria. Needs calibration against human labels. |
