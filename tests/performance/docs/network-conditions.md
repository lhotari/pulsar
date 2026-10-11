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

# Network conditions and network faults

The launcher can run the cluster and its workloads over an impaired network: with delay, jitter, packet loss and
limited bandwidth between chosen containers, for the whole run or for a time, and with timed faults, network
partitions and connection resets. A scenario's `network` section describes them, and a file applied with the
launcher's `--extends` option adds them to any scenario:

```bash
./gradlew :tests:performance:launcher:run \
  --args='--scenario tests/performance/scenarios/iot-telemetry-small.yaml --extends network/wan-clients'
```

The [network overlays](../scenarios/network) are such files, see [Network scenarios](#network-scenarios).

The impairments act on packets in the Linux kernel, with [traffic control](https://man7.org/linux/man-pages/man8/tc.8.html)
and its [netem](https://man7.org/linux/man-pages/man8/tc-netem.8.html) queue, so TCP reacts to them as it does on a
real network: a lost segment is retransmitted, a delay grows the round trip and limits what a connection's window
carries. Nothing in the cluster or the workloads changes: no configuration, no image, no address, so the same
impairments apply to a released Pulsar, such as 4.0, with `-Pperformance.clusterPulsarImage` or `compareImages`.

The applications check every device's messages, so a run with faults also checks that every message arrives in
order: a gap or an ordering violation fails the run, and the report counts the duplicates that at-least-once
delivery allows. The gateways' producers have no send timeout, so during a partition they keep retrying, and their
pending queues fill up.

## Settings

```yaml
network:
  offloads: auto                     # optional: auto, off or on, see Segmentation offloads
  impairments:                       # a mapping by name
    gateway-wan:                     # the impairment's name in the console, the report and Grafana
      between: [gateways, brokers]   # both directions; or from: and to: for one
      delay: 20ms                    # one way
      jitter: 5ms                    # the delay varies by up to this much, keeping the packets' order
      loss: 0.1%                     # of the packets
      lossBurst: 2                   # optional: the mean number of packets lost in a row; 1, independent, by default
    gateway-uplink:
      from: gateways
      to: brokers
      rate: 20mbit                   # the senders' bandwidth to the receivers
      queue: 100ms                   # optional: the bottleneck's queue, as a time at the rate; 100ms by default,
                                     # and at least 64 full frames, which is longer below about 8 Mbit/s
    broker-1-isolated:
      between: [clients, broker-1]
      partition: true                # drops every packet
      start: 30s                     # after the measurement's start
      duration: 20s                  # optional: until the run ends without it
    reset-gateways:
      from: gateways
      to: brokers
      reset: true                    # resets the TCP connections, once
      anchor: run                    # start counts from the gateways' start instead of the measurement's
      start: 2m
    brokers-unaware:
      from: applications             # the side that drops its connections
      to: brokers                    # the side that doesn't learn it
      silentDisconnect: true         # destroys the connections without the other side knowing, once
      start: 60s
      duration: 90s                  # optional: how long the old connections' packets stay dropped
```

An impairment names its endpoints with `between: [A, B]`, which impairs both directions, or `from` and `to`, which
impairs the packets from `from` to `to`. Each side is an endpoint or a list of them:

| Endpoint | Containers |
|---|---|
| `gateways` | the gateways' container, every gateway's producers |
| `applications` | the applications' container, every application's pods |
| `clients` | both |
| `brokers`, `broker-N` | every broker, or one, from `broker-0` |
| `bookies`, `bookie-N` | every bookie, or one, from `bookie-0` |
| `zookeeper` | the cluster's metadata store |
| `configuration-store` | the configuration metadata store |
| `metadata` | both stores |

An impairment does one of three things:

- **A condition** has any of these settings:
  - `delay`, one way, with a unit: `us`, `ms`, `s` or `m`, such as `20ms` or `1.5s`;
  - `jitter`, up to the delay: each packet's delay varies within delay ± jitter. netem's jitter delays each packet
    independently, which reorders the packets whenever the jitter exceeds the time between them, and TCP takes
    reordered segments for lost ones. A real path's delay varies with its queues and keeps the order, so the jitter
    keeps it: netem then sends a packet no earlier than the one before. Under load that shifts the delays towards
    delay + jitter, since a packet waits for an earlier one with a longer delay. `reorder: true` gives netem's
    independent delays;
  - `loss`, a percentage such as `0.5%`, and `lossBurst`, the mean number of packets lost in a row, for the bursts of
    loss of a radio link or a congested queue. Bursts use netem's Gilbert-Elliott model, a state that loses no packet
    and a state that loses every packet, whose mean loss and burst length are what they are set to; netem's correlated
    random loss, 1 % with a 25 % correlation, lost almost no packet on Linux 7.1;
  - `rate`, in bits per second with a unit, `bit`, `kbit`, `mbit` or `gbit`, such as `20mbit`, and its `queue`.
- **A partition**, `partition: true`, drops every packet, silently, as a cable pull or a firewall would: the senders
  see no error, and their connections time out or wait.
- **A reset**, `reset: true`, destroys the TCP connections between the endpoints once, at its `start`, which resets
  them on both ends, as a load balancer or a NAT gateway that resets its connections does. It has no `duration`.
- **A silent disconnection**, `silentDisconnect: true`, destroys the TCP connections from `from` to `to` once, at its
  `start`, without `to` learning it: one side of the connections closes and its close never reaches the other side,
  as when a NAT gateway or a firewall that has forgotten its connections answers one side's next packets with a reset
  and drops the other side's, or a host restarts behind one. A gateway that forgets its connections without answering
  either side is a partition instead. `from` gets an error at once and reconnects, on new connections, while `to` keeps the old,
  half-open connections until something of its own, such as a keep-alive, finds them dead. With `from: applications`
  and `to: brokers`, the brokers don't learn that the consumers have gone; with `from: brokers` and `to: gateways`,
  the producers don't learn that the brokers have dropped them. It needs `from` and `to`, not `between`, and its
  `duration`, until the run ends without it, is how long the old connections' packets stay dropped: after it, `from`'s
  host answers them with a reset.

An impairment without `start` lasts the whole run: the launcher applies it before the gateways create their clients,
so that their first lookups and connections already see it, and removes it after `duration`, if it has one. The
applications have subscribed by then, over the unimpaired network, so an impairment of theirs acts on their
established connections and on their reconnections. With
`start`, it starts that long after its `anchor`:

- `measurement`, the default: when the producers start the measured messages, after the warmup. The launcher holds
  the producers at the measurement's start until it has applied the impairments that start then, and their times
  count from the producers' release.
- `run`: when the gateways start creating their clients, before the warmup.

The launcher rejects a scenario whose impairments it can't apply as written: an unknown setting or endpoint, sides
with a container in common, a jitter or a reorder without a delay, a queue without a rate, settings of a condition on
a fault, a start after the measurement's estimated end, and two conditions that act on the same pair of containers at
the same time in the same place (two delays or losses, or two rates; a rate and a delay compose). Partitions, resets
and silent disconnections may overlap conditions and each other; a partition takes precedence over a condition.

The impairments are a mapping by name, so that a file applied with `--extends` adds impairments or changes one, as
the scenario files merge mappings, and `--set network.impairments.gateway-wan.delay=40ms` changes one setting.

### Brokers in the service URL

The workloads' service URL is the first broker's, so that a partition of `broker-0` also cuts the clients off from
their lookups. `cluster.brokers.bootstrap: all` lists every broker in the URL, so that the clients can look topics up
on another broker, as a client configured with several brokers or a DNS name of several brokers does:

```yaml
cluster:
  brokers:
    replicas: 2
    bootstrap: all
```

## How the impairments are applied

The launcher runs a privileged sidecar container in the Docker engine host's network and PID namespaces, the same
way as the `perf stat` sidecar, so it works on a Linux host and in the Linux VM of Docker Desktop or OrbStack. When it
starts, it checks in a network namespace of its own that the kernel and its tools can do what the impairments need,
and the run fails before the cluster starts otherwise.

- **A delay, a jitter, a loss and a partition act on the path to the receiver.** Their netem queue is on the
  receiver's veth in the Docker engine's host, the interface that hands the packets to the receiver's container,
  with a filter on the senders' IP addresses. The packets have left the sender, so the sender's TCP stack doesn't see
  the queue: its small-queue limits don't apply, as on a real network path.
- **A rate is the senders' uplink.** Its netem queue is on the sender's own interface, with a filter on the
  receivers' IP addresses, and it limits what the sender sends to all of them together. Its `queue` is a bounded
  queue, as a real bottleneck has: what doesn't fit is dropped, and TCP slows down.
- **A reset** runs `ss -K` in the senders' network namespaces, which destroys the sockets: the sender gets
  `ECONNABORTED`, and the receiver a TCP reset.
- **A silent disconnection** lists the senders' established connections to the receivers with `ss`, adds filters
  that drop each connection's packets both ways, by its addresses and ports, on both ends' veths, in a band like a
  partition's, and then destroys the senders' sockets with `ss -K`. The senders' resets are dropped, and their new
  connections, on other ports, aren't.

Each device that impairments act on gets a `prio` qdisc with a band for each of its impairments after the three
bands of the unimpaired traffic, and each band its netem queue, before the gateways start. Applying an impairment adds
its filters, and removing it deletes them: the packets in its queue are still delivered, at their time, so its
effect ends within its delay and jitter, or its queue for a rate. A partition that starts during a condition doesn't
drop the condition's queued packets either, as packets in flight on a real path still arrive. A device takes at most
13 impairments. The impairments support IPv4, the only address family of Docker's default networks.

The path queues hold up to a million packets, far more than the traffic of one host keeps in flight, since netem's
default of a thousand packets drops a fast TCP connection's packets at a delay of a few milliseconds. netem allocates
nothing for the limit. A drop that a path queue counts beyond its loss would be its overflow; the report shows the
drops.

### Segmentation offloads

A container's interface hands the kernel's TCP segments of up to 64 KB, which the segmentation offloads (TSO and
GSO) split into packets only when they leave the host's network card, and netem sees those large segments: a loss of
1 % drops 1 % of them, each many packets, and a rate is bursty. With `offloads: auto`, the default, the launcher turns
TSO and GSO off on the interfaces of the containers that send through an impairment with a loss, a jitter or a rate,
before the gateways start and for the whole run, and restores them at the end. A delay alone keeps them.

The offloads change how much CPU the senders use for their TCP traffic, so `offloads: off` turns them off on every
container of the run: a run with `network: {offloads: off}` and no impairments then has the same offloads as a run
with a loss, for comparing the two. `offloads: on` never changes them.

### When something fails

An impairment that can't be applied or removed fails the run at once, since the run would measure something else.
The launcher removes every filter and qdisc that it installed and restores the offloads before it stops the
containers, also when the run fails, and when `PULSAR_CONTAINERS_LEAVE_RUNNING` leaves the containers running. A device
that has a qdisc that the launcher didn't install fails the run before anything is changed.

The timeline stops when the workloads have finished, so that an impairment that hasn't started by then doesn't
happen. A run whose impairments didn't do what the scenario asked exits with an error after writing its report: an
impairment that never started because the workloads finished before it, one that wasn't applied because its
containers had exited, one that failed, and a reset that matched no TCP connection. So does a run whose impairments
couldn't be removed at the end.

## Files

| File | Contents |
|---|---|
| `network-events.jsonl` | Every event, one JSON object per line, written as it happens, also in a failed run: the resolved containers and devices, the offloads, each impairment's application and removal with its queues' counters, a reset's destroyed connections, failures and the cleanup |
| `gateways/pulsar-client.log.txt`, `applications/pulsar-client.log.txt` | The Pulsar client's warnings in the workloads, such as its reconnections and timeouts, when it logged any; `PULSAR_CLIENT_LOG_LEVEL: info` in a workload's `env` logs more |
| `network-summary.json` | Each impairment's settings, status, times and its queues' final counters, and each impaired container's TCP segments sent and retransmitted during the run |

The run report's **Network conditions** section shows each impairment, when it was applied and removed relative to
the measurement's start, its queues' packets and drops, and each impaired container's TCP retransmissions. The
impairments' applications and removals are annotations in Grafana, beside the run's own.

## Checking the impairments

`./gradlew :tests:performance:launcher:networkCheck` checks that the impairments do what their settings ask on
small Alpine containers, with `ping` and `iperf3`: a delay on one destination and none on another, a timed partition,
the jitter's order, a loss and a rate, a reset, silent disconnections in both directions, a timed silent
disconnection of a target without connections, and the cleanup. It needs a Docker engine that runs privileged
containers. Run it on a new host, or after changing the impairments.

## Network scenarios

The overlays in [`scenarios/network`](../scenarios/network) model real networks and faults; each describes what it
models and what to look for. Apply one with `--extends network/<name>` on a base scenario, such as
`iot-telemetry-small` for a quick run, and compare the run with a run of the base scenario without it.
The overlays that need a broker or a bookie to fail over to run on
[`failover-cluster.yaml`](../scenarios/network/failover-cluster.yaml): two brokers, both in the service URL, and four
bookies with an ensemble, a write quorum and an ack quorum of 3, 3 and 2; the others run on the base scenario's
cluster. What they showed on `iot-telemetry-small`, at 1,000 messages a second, is in each file:

| Overlay | Models | What it showed |
|---|---|---|
| `wan-clients` | clients over a WAN, 20 ms ± 2 ms | publish latency up by the round trip; nothing else |
| `mobile-gateways` | cellular gateways, 50 ms ± 20 ms, 1 % loss in bursts | the losses double the publish p99 |
| `constrained-uplink` | the gateways' uplink below their traffic for 30 s | set the rate for the base scenario |
| `cross-zone-storage` | brokers and bookies in different zones, 1 ms | publish latency up by a bookie round trip |
| `slow-bookie` | one bookie 20 ms away | hidden by the ack quorum of 2 |
| `bookie-partition` | one bookie gone for 30 s | hidden by the ack quorum of 2; with 3, a 30 s stall, the bookie client's timeout |
| `broker-partition` | clients cut off from one broker | a gray failure: no failover, the topics wait |
| `broker-isolation` | one broker off the network for 45 s | failover after the metadata session expires; clients notice through their keep-alive |
| `metadata-partition` | one broker cut off from ZooKeeper for 10 s | nothing; longer, see `metadata-takeover` |
| `metadata-takeover` | the same for 90 s, with clients looking the topics up | the isolated broker holds producers that it can't serve until ZooKeeper comes back |
| `return-path-blackhole` | the brokers' packets to the gateways lost | TCP's acknowledgments are lost too, so both directions stall |
| `connection-resets` | load balancers resetting connections | a gateway reset costs ~0.2 s; an applications reset stalls everything |
| `silent-client-disconnect` | the clients' connections vanish without the brokers knowing | the brokers hold every Key_Shared consumer twice until their keep-alive; a 512 MB broker ran out of heap |
| `silent-broker-disconnect` | the brokers drop the connections without the clients knowing | the producers and consumers stall until their keep-alive, about 45 s |
| `lossy-storage` | 0.5 % loss in bursts between the brokers and the bookies | the publish p99.9 up by half, the max to a retransmission timeout, ~200 ms |
| `zookeeper-outage` | every broker cut off from ZooKeeper past the session timeout, with clients looking the topics up | the brokers keep serving their topics; the lookups' reset costs a few seconds |
| `constrained-consumers` | the consumers' downlink below their traffic for 60 s | a 512 MB broker ran out of heap with the unacknowledged dispatches; with 4 GB, the untouched producers' latency still reached 4 s |
| `consumer-reconnect-storm` | every consumer reconnecting at once | unrelated producers stall for 1-3 s while the brokers resubscribe the Key_Shared consumers |

## Why not a proxy

A TCP proxy, such as [Toxiproxy](https://github.com/Shopify/toxiproxy), terminates the TCP connections, so its
latency and bandwidth act on two separate connections rather than on one end-to-end path, its packet loss drops chunks
of the byte stream after TCP has received them, which no retransmission recovers, and it copies every byte through a
process on the same host. It also needs the clients to connect through it: Pulsar's clients connect to the broker
that a lookup returns, so the brokers would need [advertised listeners](https://pulsar.apache.org/docs/next/concepts-multiple-advertised-listeners/)
for the proxy, the bookies their own ports, and the metadata store URLs would point at it, which changes the cluster
under test. Faults inside the byte stream, such as a connection that stops after some bytes, need a proxy, and aren't
supported.
