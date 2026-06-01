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
package org.apache.pulsar.common.tls;

/**
 * A listener for TLS material updates (PIP-478).
 *
 * <p>The provider calls {@link #onTlsMaterialUpdated} once when the material for a purpose is first
 * ready, then again whenever it changes. This is mainly useful on the broker side, where the server
 * must rebuild its listener {@code SSLContext} on rotation; on the client side the framework
 * typically re-requests {@link PulsarTlsMaterialProvider#getTlsMaterial} when a new connection is
 * created.
 */
@FunctionalInterface
public interface TlsMaterialListener {

    /**
     * Called when the material for a purpose becomes ready and on every subsequent change.
     *
     * @param purpose  the purpose whose material changed
     * @param material the new material
     */
    void onTlsMaterialUpdated(TlsPurposeContext purpose, TlsMaterial material);
}
