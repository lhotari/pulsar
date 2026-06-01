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
 * A source of {@link TlsMaterial} for the default {@link FileBasedTlsMaterialProvider} (PIP-478).
 *
 * <p>A source encapsulates loading the material for one purpose from some backing store (PEM files,
 * a keystore, ...) and reloading it when the backing store changes. Implementations return a new,
 * non-equal {@link TlsMaterial} instance when the material has changed since the last call, and the
 * same instance otherwise, so the provider can detect rotations and notify listeners efficiently.
 */
public interface TlsMaterialSource {

    /**
     * Return the current material, reloading from the backing store if it changed.
     *
     * @return the current TLS material
     * @throws Exception if the material cannot be loaded
     */
    TlsMaterial getTlsMaterial() throws Exception;

    /**
     * @return {@code true} if this source produces server-side material, {@code false} for client-side
     */
    boolean isServer();
}
