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
package org.apache.pulsar.broker.service;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Linearizes new connection/entity admission with shutdown acceptance. No user code runs under its monitor.
 * Existing admitted entities are independent of this gate and keep serving until their topic is unloaded.
 */
public final class BrokerAdmission {
    private Set<Ticket> pending = new HashSet<>();
    private volatile boolean closed;

    public boolean isClosed() {
        return closed;
    }

    /** Register a connection or entity request. A null ticket means that shutdown has already won. */
    public synchronized Ticket register(Executor executor, Runnable rejected) {
        if (closed) {
            return null;
        }
        Ticket ticket = new Ticket(executor, rejected);
        pending.add(ticket);
        return ticket;
    }

    /** Publish the irreversible cutoff, returning cancellation dispatches to run outside the acceptance lock. */
    public synchronized Iterable<Runnable> close() {
        closed = true;
        Set<Ticket> rejected = pending;
        pending = new HashSet<>();
        // Iteration and executor dispatch happen after releasing the gate. Even a large pending set must
        // not delay installing the watchdog or hold the admission monitor while callbacks are scheduled.
        return () -> rejected.stream()
                .<Runnable>map(ticket -> () -> ticket.executor.execute(ticket.rejected)).iterator();
    }

    public final class Ticket implements AutoCloseable {
        private final Executor executor;
        private final Runnable rejected;

        private Ticket(Executor executor, Runnable rejected) {
            this.executor = executor;
            this.rejected = rejected;
        }

        /** Commit before sending success, completing futures, or invoking entity callbacks. */
        public boolean commit() {
            synchronized (BrokerAdmission.this) {
                return !closed && pending.remove(this);
            }
        }

        /** Forget a failed or canceled request without running its rejection callback. */
        @Override
        public void close() {
            synchronized (BrokerAdmission.this) {
                pending.remove(this);
            }
        }
    }
}
