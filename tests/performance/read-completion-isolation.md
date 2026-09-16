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
12 million unbatched 128-byte messages, and unrestricted production. Each producer has 40 outstanding
sends, for at most 20,000 across 500 producers. The consumer subscription is created before publishing.

Allow at least 16 GiB of available host memory, sufficient Docker disk space, and 10 minutes for a
run after building. The broker has a 2 GiB heap and 4 GiB direct-memory ceiling. Run comparisons
sequentially on the same idle host and keep CPU governor, JDK, image build settings and profiler
options identical. A timeout, process restart or OOM invalidates the run even if a client reconnects.

## Baseline and candidate

The baseline is current master plus the prerequisite ledger-callback ordering change in PR #26599
and the managed-ledger cache allocator change in PR #26603.
For the initial review it is `lh-perfopt-ledger-ordering-key-base`; the candidate is
`lh-perfopt-read-completion-isolation`. Both must run the same harness and YAML. To prepare a separate
baseline checkout with only the candidate's test scaffolding:

```bash
git worktree add ../pulsar-read-baseline lh-perfopt-ledger-ordering-key-base
git diff lh-perfopt-ledger-ordering-key-base lh-perfopt-read-completion-isolation -- \
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
- In `produce-v4.*.txt` and `consume-v4.*.txt`, check the final counts (12 million), errors, throughput
  and latency. Consumer throughput during publication and backlog growth are the primary signals;
  producer capacity alone can hide a stalled consumer. Whole-run consumer throughput includes startup
  and drain time.
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
