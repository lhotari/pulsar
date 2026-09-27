#!/bin/sh
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

# NOTE: this script is intentionally not executable. It is only meant to be sourced for environment variables.

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# default settings for starting bookkeeper

# Configuration file of settings used in bookie server
BOOKIE_CONF=${BOOKIE_CONF:-"$BK_HOME/conf/bookkeeper.conf"}

# Log4j configuration file
# BOOKIE_LOG_CONF=

# Logs location
BOOKIE_LOG_DIR=${BOOKIE_LOG_DIR:-"${PULSAR_LOG_DIR}"}

# Memory size options
BOOKIE_MEM=${BOOKIE_MEM:-${PULSAR_MEM:-"-Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g"}}

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

# Netty buffer allocator and recycler settings. conf/pulsar_env.sh sets the same options, and its values take
# precedence in the scripts that source both files (bin/pulsar for commands other than bookie, and bin/pulsar-perf).
# BOOKIE_EXTRA_OPTS overrides them. The settings and their default values are defined in:
#   https://github.com/netty/netty/blob/4.2/buffer/src/main/java/io/netty/buffer/AdaptiveByteBufAllocator.java
#   https://github.com/netty/netty/blob/4.2/buffer/src/main/java/io/netty/buffer/PooledByteBufAllocator.java
#   https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/Recycler.java

# Allocators: Netty's adaptive allocator for Pulsar's default allocator and for Netty's own default allocator (Netty
# 4.2's default). See conf/pulsar_env.sh for the allocator settings.
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
BOOKIE_GC="${BOOKIE_GC:-${PULSAR_GC}}"
if [ -z "$BOOKIE_GC" ]; then
  BOOKIE_GC="-XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch"
  if [[ $JAVA_MAJOR_VERSION -eq 21 || $JAVA_MAJOR_VERSION -eq 22 ]]; then
    BOOKIE_GC="-XX:+UseZGC -XX:+ZGenerational ${BOOKIE_GC}"
  else
    BOOKIE_GC="-XX:+UseZGC ${BOOKIE_GC}"
  fi
fi

if [[ -z "$BOOKIE_GC_LOG" ]]; then
  # fallback to PULSAR_GC_LOG if it is set
  BOOKIE_GC_LOG="$PULSAR_GC_LOG"
fi

BOOKIE_GC_LOG_DIR=${BOOKIE_GC_LOG_DIR:-"${PULSAR_GC_LOG_DIR:-"${BOOKIE_LOG_DIR}"}"}

if [[ -z "$BOOKIE_GC_LOG" ]]; then
  if [[ $JAVA_MAJOR_VERSION -gt 8 ]]; then
    BOOKIE_GC_LOG="-Xlog:gc*,safepoint:${BOOKIE_GC_LOG_DIR}/pulsar_bookie_gc_%p.log:time,uptime,tags:filecount=10,filesize=20M"
    if [[ $JAVA_MAJOR_VERSION -ge 17 ]]; then
      # Use async logging on Java 17+ https://bugs.openjdk.java.net/browse/JDK-8264323
      BOOKIE_GC_LOG="-Xlog:async ${BOOKIE_GC_LOG}"
    fi
  else
    # Java 8 gc log options
    BOOKIE_GC_LOG="-Xloggc:${BOOKIE_GC_LOG_DIR}/pulsar_bookie_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=20M"
  fi
fi

BOOKIE_EXTRA_OPTS="${BOOKIE_EXTRA_OPTS} ${PULSAR_EXTRA_OPTS}"

# Add extra paths to the bookkeeper classpath
# BOOKIE_EXTRA_CLASSPATH=

#Folder where the Bookie server PID file should be stored
#BOOKIE_PID_DIR=

#Wait time before forcefully kill the Bookie server instance, if the stop is not successful
#BOOKIE_STOP_TIMEOUT=

#Entry formatter class to format entries.
#ENTRY_FORMATTER_CLASS=
