#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# default settings for starting pulsar broker

# Log4j configuration file
# PULSAR_LOG_CONF=

# Logs location
# PULSAR_LOG_DIR=

# Log format: "text" (default) or "json" (flat OpenTelemetry JSON, useful for log aggregators)
# PULSAR_LOG_FORMAT=json

# Configuration file of settings used in broker server
# PULSAR_BROKER_CONF=

# Configuration file of settings used in bookie server
# PULSAR_BOOKKEEPER_CONF=

# Configuration file of settings used in zookeeper server
# PULSAR_ZK_CONF=

# Configuration file of settings used in global zookeeper server
# PULSAR_GLOBAL_ZK_CONF=

# Select the ByteBuf allocator by adding -Dpulsar.allocator.type=<value> to PULSAR_EXTRA_OPTS.
# Supported values (case-insensitive):
#   pooled   - Netty PooledByteBufAllocator; prefers direct buffers (built-in default).
#   unpooled - Netty UnpooledByteBufAllocator; prefers heap buffers.
#   adaptive - Netty AdaptiveByteBufAllocator; auto-tunes pooling and prefers direct buffers
#              (set for the default allocator below; override it with -Dpulsar.allocator.default.type=pooled,
#              since the named setting takes precedence).
# Named allocators override each setting independently with pulsar.allocator.<id>.<setting>:
#   pulsar.allocator.default.type  - allocator used by general Pulsar operations
#   pulsar.allocator.ml-cache.type - separate allocator used for managed-ledger cache copies (default: adaptive)
# Supported settings: type, exit_on_oom (false), out_of_memory_policy (FallbackToHeap or ThrowException).
# Named settings fall back to the unqualified pulsar.allocator.<setting>, then the built-in default.
# Explicit global type and legacy pooled settings also apply to ml-cache unless overridden by name.
# Batch reads copy entries into this cache even when managedLedgerCacheCopyEntries=false. Adaptive
# reuses small size-class slots to limit fragmentation; retained chunks and size rounding still cost memory.
# To retain the previous cache allocator: -Dpulsar.allocator.ml-cache.type=pooled
# Settings are read when an allocator is first created. default overrides do not apply to other IDs.
# Leak detection is global: use -Dio.netty.leakDetection.level=disabled|simple|advanced|paranoid.
# pulsar.allocator.leak_detection and per-allocator leak_detection settings are not supported.
# -Dpulsar.allocator.pooled=true is deprecated; use -Dpulsar.allocator.type=pooled instead.
# pulsar.allocator.type takes precedence over the legacy pulsar.allocator.pooled property.
# If pulsar.allocator.type is unset, pulsar.allocator.pooled=true (or unset) selects pooled;
# other values of pulsar.allocator.pooled select unpooled.

# Extra options to be passed to the jvm
PULSAR_MEM=${PULSAR_MEM:-"-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g"}

if [ -z "$JAVA_HOME" ]; then
  JAVA_BIN=java
else
  JAVA_BIN="$JAVA_HOME/bin/java"
fi
for token in $("$JAVA_BIN" -version 2>&1 | grep 'version "'); do
    if [[ $token =~ \"([[:digit:]]+)\.([[:digit:]]+)(.*)\" ]]; then
        if [[ ${BASH_REMATCH[1]} == "1" ]]; then
          JAVA_MAJOR_VERSION=${BASH_REMATCH[2]}
        else
          JAVA_MAJOR_VERSION=${BASH_REMATCH[1]}
        fi
        break
    elif [[ $token =~ \"([[:digit:]]+)(.*)\" ]]; then
        # Process the java versions without dots, such as `17-internal`.
        JAVA_MAJOR_VERSION=${BASH_REMATCH[1]}
        break
    fi
done

# Netty buffer allocator and recycler settings. conf/bkenv.sh sets the same options. Scripts that source both files
# (bin/pulsar for commands other than bookie, and bin/pulsar-perf) source this file last, so its values take precedence.
# PULSAR_EXTRA_OPTS overrides them. The settings and their default values are defined in:
#   https://github.com/netty/netty/blob/4.2/buffer/src/main/java/io/netty/buffer/AdaptiveByteBufAllocator.java
#   https://github.com/netty/netty/blob/4.2/buffer/src/main/java/io/netty/buffer/PooledByteBufAllocator.java
#   https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/Recycler.java

# Allocators: Netty's adaptive allocator for Pulsar's default allocator and for Netty's own default allocator (Netty
# 4.2's default). See the allocator settings above.
OPTS="$OPTS -Dpulsar.allocator.default.type=adaptive -Dio.netty.allocator.type=adaptive"

# PooledByteBufAllocator: the chunk size is pageSize * 2^maxOrder, where pageSize is io.netty.allocator.pageSize
# (default 8 KiB) and maxOrder is io.netty.allocator.maxOrder (default 9): 8 KiB * 2^9 = 4 MiB by default. Allocations
# larger than a chunk bypass the pool, and Pulsar's default maximum message size is 5 MB, so set maxOrder to 10 for
# 8 KiB * 2^10 = 8 MiB chunks to pool such messages and reduce native memory fragmentation.
# Only FastThreadLocalThreads and event-loop threads get a thread cache. io.netty.allocator.useCacheForAllThreads stays
# false: other threads don't release their cache when they end, so it would stay allocated until a finalizer frees it.
OPTS="$OPTS -Dio.netty.allocator.maxOrder=10"

# AdaptiveByteBufAllocator: by default, only event-loop threads get thread-local magazines. Also give them to the other
# threads that remove their FastThreadLocals when they end (Pulsar's threads, see "Creating threads" in CODING.md, and
# the common ForkJoinPool's workers below), which free them then. Other threads use the shared magazines. By default
# (io.netty.allocator.lowMemory), no thread-local magazines are used when the maximum heap size is 512 MiB or less.
OPTS="$OPTS -Dio.netty.allocator.useCachedMagazinesForNonEventLoopThreads=true"

# Recycler: for each recycled object type, a thread keeps up to maxCapacityPerThread objects (default 4096) in a queue
# that grows in chunks of chunkSize entries (default 32), plus a thread-local batch of up to chunkSize objects. Larger
# chunks mean fewer chunk allocations and larger batches.
OPTS="$OPTS -Dio.netty.recycler.maxCapacityPerThread=4096 -Dio.netty.recycler.chunkSize=256"

# The common ForkJoinPool runs CompletableFuture's *Async methods by default. Up to Java 23, run its workers like
# FastThreadLocalThreads, so that they use the recycler and allocator thread caches and release them when they end.
# From Java 24 on, the JDK's default common-pool workers run in their own thread group and clear their ThreadLocals.
# Workers that cleared them would drop Netty's thread-local caches without releasing them, so keep the JDK's workers.
if [[ $JAVA_MAJOR_VERSION -lt 24 ]]; then
  OPTS="$OPTS -Djava.util.concurrent.ForkJoinPool.common.threadFactory=org.apache.pulsar.common.util.netty.FastThreadLocalForkJoinWorkerThreadFactory"
fi

# Garbage collection options
if [ -z "$PULSAR_GC" ]; then
  PULSAR_GC="-XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch"
  if [[ $JAVA_MAJOR_VERSION -eq 21 || $JAVA_MAJOR_VERSION -eq 22 ]]; then
    PULSAR_GC="-XX:+UseZGC -XX:+ZGenerational ${PULSAR_GC}"
  else
    PULSAR_GC="-XX:+UseZGC ${PULSAR_GC}"
  fi
fi

PULSAR_GC_LOG_DIR=${PULSAR_GC_LOG_DIR:-"${PULSAR_LOG_DIR}"}

if [[ -z "$PULSAR_GC_LOG" ]]; then
  if [[ $JAVA_MAJOR_VERSION -gt 8 ]]; then
    PULSAR_GC_LOG="-Xlog:gc*,safepoint:${PULSAR_GC_LOG_DIR}/pulsar_gc_%p.log:time,uptime,tags:filecount=10,filesize=20M"
    if [[ $JAVA_MAJOR_VERSION -ge 17 ]]; then
      # Use async logging on Java 17+ https://bugs.openjdk.java.net/browse/JDK-8264323
      PULSAR_GC_LOG="-Xlog:async ${PULSAR_GC_LOG}"
    fi
  else
    # Java 8 gc log options
    PULSAR_GC_LOG="-Xloggc:${PULSAR_GC_LOG_DIR}/pulsar_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=20M"
  fi
fi

# Add extra paths to the bookkeeper classpath
# PULSAR_EXTRA_CLASSPATH=

#Folder where the Bookie server PID file should be stored
#PULSAR_PID_DIR=

#Wait time before forcefully kill the pulsar server instance, if the stop is not successful
#PULSAR_STOP_TIMEOUT=

# Enable semantically stable telemetry for JVM metrics, unless otherwise overridden by the user.
if [ -z "$OTEL_SEMCONV_STABILITY_OPT_IN" ]; then
  export OTEL_SEMCONV_STABILITY_OPT_IN=jvm
fi
