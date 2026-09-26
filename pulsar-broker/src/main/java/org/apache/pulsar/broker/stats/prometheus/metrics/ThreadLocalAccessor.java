/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.stats.prometheus.metrics;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.jspecify.annotations.Nullable;

class ThreadLocalAccessor {

    private final ConcurrentHashMap<LocalData, Boolean> map = new ConcurrentHashMap<>();
    // Per-instance by design: each metric logger keeps its own per-thread sketches. Loggers are long-lived and
    // created once per metric name or label set, so the number of FastThreadLocal indexes this takes is bounded.
    private final FastThreadLocal<LocalData> localData = new FastThreadLocal<>() {

        @Override
        protected LocalData initialValue() {
            LocalData localData = new LocalData();
            map.put(localData, Boolean.TRUE);
            return localData;
        }

        @Override
        protected void onRemoval(LocalData value) {
            // Keep the LocalData in the map so that the next record() call merges the values recorded since the last
            // call before removing it.
            value.markOwnerThreadTerminated();
        }
    };

    void record(KllDoublesSketch aggregateSuccess, @Nullable KllDoublesSketch aggregateFail) {
        map.keySet().forEach(key -> {
            key.record(aggregateSuccess, aggregateFail);
            if (key.shouldRemove()) {
                map.remove(key);
            }
        });
    }

    LocalData getLocalData() {
        return localData.get();
    }

    @VisibleForTesting
    int getLocalDataCount() {
        return map.keySet().size();
    }

    static class LocalData {

        private final KllDoublesSketch successSketch = KllDoublesSketch.newHeapInstance();
        private final KllDoublesSketch failSketch = KllDoublesSketch.newHeapInstance();
        private final StampedLock lock = new StampedLock();
        // Keep a weak reference to the owner thread so that we can remove the LocalData when the thread
        // is not alive anymore or has been garbage collected.
        // This reference isn't needed when the owner thread removes its FastThreadLocals when it ends, and will be
        // null in that case: FastThreadLocal#onRemoval sets ownerThreadTerminated.
        private final WeakReference<Thread> ownerThreadReference;
        private volatile boolean ownerThreadTerminated;

        LocalData() {
            if (FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
                ownerThreadReference = null;
            } else {
                ownerThreadReference = new WeakReference<>(Thread.currentThread());
            }
        }

        void markOwnerThreadTerminated() {
            ownerThreadTerminated = true;
        }

        private boolean shouldRemove() {
            if (ownerThreadTerminated) {
                // the owner thread has removed its FastThreadLocals when it ended
                return true;
            } else if (ownerThreadReference == null) {
                // the owner thread will set ownerThreadTerminated using FastThreadLocal#onRemoval
                return false;
            } else {
                Thread ownerThread = ownerThreadReference.get();
                if (ownerThread == null) {
                    // the thread has already been garbage collected, LocalData should be removed
                    return true;
                } else {
                    // the thread isn't alive anymore, LocalData should be removed
                    return !ownerThread.isAlive();
                }
            }
        }

        void record(KllDoublesSketch aggregateSuccess, @Nullable KllDoublesSketch aggregateFail) {
            long stamp = lock.writeLock();
            try {
                aggregateSuccess.merge(successSketch);
                successSketch.reset();
                if (aggregateFail != null) {
                    aggregateFail.merge(failSketch);
                    failSketch.reset();
                }
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        void updateSuccess(double value) {
            long stamp = lock.readLock();
            try {
                successSketch.update(value);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        void updateFail(double value) {
            long stamp = lock.readLock();
            try {
                failSketch.update(value);
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }
}
