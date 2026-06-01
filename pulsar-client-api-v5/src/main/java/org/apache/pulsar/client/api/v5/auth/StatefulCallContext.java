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
package org.apache.pulsar.client.api.v5.auth;

import java.util.Optional;

/**
 * A per-call context exposing an implementation-controlled state slot for retaining conversation
 * state across calls (challenge/response rounds, multi-stage HTTP auth) (PIP-478).
 */
public interface StatefulCallContext {

    /**
     * Retrieve an implementation-controlled state object previously stored with
     * {@link #setStateObject}.
     *
     * <p>The slot is keyed by class so multiple implementations can coexist without collision;
     * implementations typically store one object of their own type (for example their SASL
     * conversation state).
     *
     * <p>The slot's lifetime equals the authentication exchange: for the binary protocol, the
     * lifetime of one {@code ClientCnx} setup (including all {@code CommandAuthChallenge} /
     * {@code CommandAuthResponse} rounds); for HTTP, one request's retry sequence. Concurrent
     * authentications to different brokers get their own context with their own slots, so multiple
     * in-flight handshakes don't collide.
     *
     * @param clazz the state object's key class
     * @param <T>   the state object type
     * @return the stored object, if present
     */
    <T> Optional<T> getStateObject(Class<T> clazz);

    /**
     * Store a state object keyed by its (or any) class.
     *
     * @param clazz the key class
     * @param value the state object
     * @param <T>   the state object type
     */
    <T> void setStateObject(Class<T> clazz, T value);
}
