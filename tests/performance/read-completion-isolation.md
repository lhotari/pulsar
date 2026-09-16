<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Read-completion queue isolation

An Exclusive/Failover dispatcher already moves its read continuation to its own executor. A cache
hit on that executor need not first queue its completion behind producer work on the managed-ledger
executor. The change lets this future adapter opt into inline successful completion, while keeping
`whenCompleteAsync` on the dispatcher. Other callbacks retain their existing affinity. Failure and
compacted-read paths are unchanged.

## Workload

[read-completion-isolation.yaml](read-completion-isolation.yaml) uses `produce-v4` and `consume-v4`
from the standard profiling harness: 500 producers, 500 isolated clients (shared PIP-234 resources),
one connection per client, a single non-partitioned `persistent://` topic, one Exclusive consumer,
a target of 12 million unbatched 128-byte messages, and unrestricted production. Each producer has
40 outstanding sends, for at most 20,000 across 500 producers. The consumer subscription is created before publishing.

Allow at least 16 GiB of available host memory, sufficient Docker disk space, and 10 minutes for a
run after building. The broker has a 2 GiB heap and 4 GiB direct-memory ceiling. Run comparisons
sequentially on the same idle host and keep CPU governor, JDK, image build settings and profiler
options identical. A timeout, process restart or OOM invalidates the run even if a client reconnects.

## Baseline and candidate

The baseline is `origin/master`, including the merged ledger-callback ordering and managed-ledger
cache allocator changes. The candidate is `lh-perfopt-read-completion-isolation`.
Both must run the same harness and YAML. To prepare a separate
baseline checkout with only the candidate's test scaffolding:

```bash
git worktree add --detach ../pulsar-read-baseline origin/master
git diff origin/master lh-perfopt-read-completion-isolation -- \
  tests/integration tests/performance > /tmp/read-completion-harness.patch
git -C ../pulsar-read-baseline apply /tmp/read-completion-harness.patch
```

Run the following from each checkout's root, changing `baseline` to `candidate` for the improved
version. Use an absolute YAML path because Gradle runs the test from the integration module directory.
Do not build/run both checkouts concurrently: they share Docker image tags.

```bash
PULSAR_PROFILING_CONFIG="$PWD/tests/performance/read-completion-isolation.yaml" \
PULSAR_PROFILING_OUTPUT_DIRECTORY="$PWD/tests/integration/build/pulsar-profiling/baseline" \
./gradlew :tests:integration:profilingIntegrationTest --tests '*PulsarProfilingV4Test' \
  '-Pinttest.asyncprofiler.opts=event=cpu,interval=10ms,wall=50ms,lock=1ms,alloc=2m,jfrsync=profile'
```

For a rate-limited comparison add `PULSAR_PROFILING_LOAD_PRODUCE_RATE=100000`. At 500 clients this
is 200 messages/s per client. Keep the message count divisible by the number of producer clients;
the harness rejects combinations that could leave the consumer waiting forever. To repeat, use a
fresh output directory for every run. Record `git rev-parse HEAD` and `git diff` alongside the results.

## What to compare

- In `stats-v4.*.txt`, verify 500 publishers with distinct connection addresses, one Exclusive
  consumer, steady `msgRateIn`/`msgRateOut`, and the peak and final `msgBacklog` for subscription `sub`.
- In `produce-v4.*.txt` and `consume-v4.*.txt`, check the final counts (at least 12 million),
  errors, throughput and latency. Consumer throughput during publication and backlog growth are the primary signals;
  producer capacity alone can hide a stalled consumer. Whole-run consumer throughput includes startup
  and drain time. The producer counts completed sends, so it can overshoot the target by sends still
  in flight; record the actual totals rather than assuming an exact production count.
- Derive publishing and dispatch rates from differences in `msgInCounter` and `msgOutCounter`
  divided by the time between snapshots. The `msgRateIn`/`msgRateOut` fields are cached broker rates
  and can lag a phase change. For a steady window, omit the first 20 seconds after all 500 publishers
  appear and stop before publishers start disconnecting.
- Analyze each broker JFR with Jafar `jfr_diagnose` and `jfr_stackprofile`; save the results beside it
  as `<filename>.jfr.analysis.md`. Include CPU and `profiler.WallClockSample` views, and distinguish
  startup/drain from steady publishing. Look for a busy ledger worker alongside a parked subscription
  dispatcher in the baseline, and resumed dispatch progress with the change. Also compare allocation
  samples and GC pauses; concurrent GC duration is not a pause metric.

The handoff-only microbenchmark provides a faster check of queue/future overhead:

```bash
./gradlew :microbench:shadowJar
java -jar microbench/build/libs/microbench-*-benchmarks.jar \
  '.*ReadCompletionHandoffBenchmark.*' -prof gc -rf json -rff read-handoff.json
```

It compares the two completion paths with real executors, without storage IO or publishing pressure.
It does not predict end-to-end throughput or prove concurrency correctness. The managed-ledger tests
cover callback affinity, the asynchronous dispatch boundary, cache misses across ledger rollover,
and read failure/retry. Broker dispatcher and failover tests provide additional coverage.

## Observed comparison

A local comparison against current master, with performance power settings and SMT enabled
completed both runs with 500 distinct producer connections, 12 million messages consumed and no
failed ACKs:

| Metric | Baseline | Inline read completion |
|---|---:|---:|
| Steady ingress, from counter deltas | 106,980 msg/s | 119,048 msg/s |
| Steady dispatch, from counter deltas | 4,581 msg/s | 119,057 msg/s |
| Whole-run producer throughput | 99,762 msg/s | 108,766 msg/s |
| Whole-run consumer throughput | 66,254 msg/s | 113,550 msg/s |
| Sampled peak backlog | 11,375,512 | 13,139 |
| Broker CPU in a steady 30-second window | 6.27 logical cores | 5.35 logical cores |

The baseline dispatcher was sleeping in 601 of 602 weighted wall samples while the ledger worker
was nearly fully busy. After the change, dispatch kept up with publishing. The handoff JMH measured
9.27 to 4.46 microseconds per operation and 312 to 232 bytes per operation.

This is one pair of runs on an 8-core / 16-thread i9-9980HK. Both runs experienced thermal throttling,
so the exact capacity differences need repetition. The steady windows reached 97C and 96C, respectively. The large dispatch/backlog difference is the main
result. Heap allocation per second increased as the candidate performed much more delivery work;
do not infer a macro allocation reduction from the handoff benchmark. When analyzing ZGC recordings,
use `jdk.GCPhasePause` for pauses: total `jdk.GarbageCollection` duration includes concurrent work.
