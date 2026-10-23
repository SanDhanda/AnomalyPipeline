# Transaction Anomaly Detection Pipeline

A simulated banking pipeline that streams fake card transactions through parallel workers to flag unusually large spends, while handling duplicates, failures and retries along the way.

It is an in-process stand-in for a Kafka-style system, written in Go. Channels play the role of topics and partitions, so the whole thing runs with no infrastructure, but the design follows the patterns used in real event-driven systems.

## What it demonstrates

* **Partitioning by key.** Transactions are routed by account ID, so every transaction for an account is handled by the same worker, in order.
* **Consumer parallelism.** One worker per partition. More partitions means more parallel scoring, up to the number of CPU cores available.
* **Backpressure.** Partition channels are bounded, so a slow worker makes the producer wait instead of memory growing without limit.
* **Idempotent processing.** The same event delivered twice is detected by its event ID and skipped.
* **Retries and a dead-letter queue.** Transient scoring failures are retried with backoff. Events that keep failing are parked in a dead-letter channel instead of blocking the pipeline.
* **A simple anomaly model.** Each account's normal spend is tracked with a running mean and standard deviation, and large deviations (z-scores) are flagged.
* **Evaluation against ground truth.** The generator knows which transactions it made anomalous, so the program reports precision and recall.

## Architecture

```
                         +--> partition 0 --> scorer 0 --+
                         |                               |
Producer --(hash of      +--> partition 1 --> scorer 1 --+--> alerts -------> Alert service
 account ID)-----------> |                               |
                         +--> partition 2 --> scorer 2 --+--> dead letters -> Dead-letter service
                         |                               |
                         +--> partition 3 --> scorer 3 --+
```

### Components

**Producer.** Generates synthetic transactions for 50 accounts. Each account has its own normal spending level. After a warm-up period, a small fraction of transactions are made 10 times larger than normal, and these are the anomalies the system should catch. About 1% of events are sent twice to simulate at-least-once delivery. The producer hashes the account ID (FNV-1a) to choose a partition, and closes all partitions when finished.

**Partitions.** Bounded Go channels. Because an account always maps to the same partition, each worker owns its accounts' state exclusively.

**Scoring workers.** One goroutine per partition. For each transaction a worker:

1. Skips it if the event ID has already been processed.
2. Calls the scoring step, retrying up to 3 times with backoff if it fails transiently.
3. Sends the event to the dead-letter channel if all attempts fail.
4. Otherwise records the event as seen, then compares the amount with the account's history. If the z-score exceeds the threshold it raises an alert. If not, the amount is added to the account's history, so the model only learns from transactions it believes are normal.

**Alert service.** Counts alerts and compares them with the ground-truth label to track true and false positives. Prints the first few alerts.

**Dead-letter service.** Counts and prints events that failed all retries.

### The scoring "model"

Per account, the program keeps a running mean and variance (Welford's algorithm), so no history needs to be stored. An account is not scored until it has at least 10 observations. A transaction is flagged when:

```
|amount - mean| / standard deviation > 4
```

The model is deliberately simple. It sits behind a single function, so it could be swapped for a trained model (for example an Isolation Forest) without changing the rest of the pipeline.

## Running it

Requires Go 1.22 or later.

```
go mod init anomalypipeline   # first time only
go run . 
```

|Flag|Default|Meaning|
|-|-|-|
|`-partitions`|4|Number of partitions (and scoring workers)|
|`-events`|5000|Number of transactions to generate|

To see the effect of parallelism, compare throughput between runs:

```
go run . -partitions 1 -events 5000
go run . -partitions 4 -events 5000
```

Each scoring call includes about 1 ms of simulated inference cost, so extra partitions produce a visible speed-up on a machine with spare cores.



## Output

At the end of the run the program prints a summary:

```
Partitions / workers:    ...
Events published:        ... (includes deliberate duplicates)
Duplicates skipped:      ...
Processed:               ...
Transient retries:       ...
Dead-lettered:           ...
Anomalies injected:      ...
Alerts raised:           ... (true ..., false ...)
Precision / recall:      ...% / ...%
Elapsed:                 ...s
Throughput:              ... events/sec
```

## Design notes

* **No locks in the workers.** Each worker's seen-set and per-account history are private to it. Partitioning by account guarantees no other worker touches the same account.
* **Shared counters use atomics.** Counters updated by several workers use `sync/atomic`. Counters written by a single goroutine are plain integers and are only read after that goroutine has finished.
* **Shutdown order matters.** The producer closes the partitions, workers drain and exit, and only then are the alert and dead-letter channels closed. Closing them earlier would cause a worker to send on a closed channel.
* **An event is marked as seen only after it succeeds.** A dead-lettered event is therefore not treated as processed, and could be replayed later.
* **Anomalies are excluded from learning.** Flagged transactions are not added to an account's history, so one large spend does not distort the account's baseline.



## Limitations

* **In-memory only.** There is no network, persistence or real broker. If the process stops, all state is lost.
* **Unbounded seen-set.** The set of processed event IDs grows forever. A real system would expire old IDs or use a bounded store.
* **Synthetic data.** Spending is drawn from simple per-account normal distributions, so the results say nothing about performance on real transactions.
* **Simple model.** A per-account z-score cannot capture patterns such as unusual merchants, locations or timing.
* **Money as floating point.** Amounts are `float64` for simplicity. A real system would use integer pence or a decimal type.
* **Parallelism is capped by partition count.** Adding more workers than partitions does nothing.

## 

