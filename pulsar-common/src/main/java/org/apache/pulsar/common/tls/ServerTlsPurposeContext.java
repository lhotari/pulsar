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

import java.util.Objects;
import java.util.Optional;

/**
 * A server-side {@link TlsPurposeContext} (PIP-478), carrying the broker-only listener/host context.
 *
 * <p>Different advertised listeners can carry different material — for example {@code internal} for
 * in-cluster broker-to-broker traffic and {@code external} for a publicly-trusted CA without mTLS.
 */
public final class ServerTlsPurposeContext implements TlsPurposeContext {

    /** Well-known server purposes. */
    public enum ServerPurpose {
        BROKER, PROXY, WEB_SERVICE
    }

    private final ServerPurpose purpose;
    private final String advertisedListenerName;
    private final String host;
    private final UsageIdentifier usageIdentifier;

    /**
     * @param purpose the well-known server purpose
     */
    public ServerTlsPurposeContext(ServerPurpose purpose) {
        this(purpose, null, null, null);
    }

    /**
     * @param purpose                the well-known server purpose
     * @param advertisedListenerName the advertised-listener name when listener-specific (nullable)
     * @param host                   the host the material is served for, when relevant (nullable)
     * @param usageIdentifier        an optional finer-grained usage identifier (nullable)
     */
    public ServerTlsPurposeContext(ServerPurpose purpose, String advertisedListenerName, String host,
                                   UsageIdentifier usageIdentifier) {
        this.purpose = Objects.requireNonNull(purpose, "purpose");
        this.advertisedListenerName = advertisedListenerName;
        this.host = host;
        this.usageIdentifier = usageIdentifier;
    }

    /**
     * Convenience factory for a purpose with no listener/host/usage identifier.
     *
     * @param purpose the well-known server purpose
     * @return a new context
     */
    public static ServerTlsPurposeContext of(ServerPurpose purpose) {
        return new ServerTlsPurposeContext(purpose);
    }

    /**
     * @return the well-known server purpose
     */
    public ServerPurpose purpose() {
        return purpose;
    }

    /**
     * @return the advertised-listener name (e.g. {@code "internal"} / {@code "external"}), if any
     */
    public Optional<String> advertisedListenerName() {
        return Optional.ofNullable(advertisedListenerName);
    }

    /**
     * @return the host the material is served for, if relevant
     */
    public Optional<String> host() {
        return Optional.ofNullable(host);
    }

    @Override
    public boolean isServer() {
        return true;
    }

    @Override
    public Optional<UsageIdentifier> usageIdentifier() {
        return Optional.ofNullable(usageIdentifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServerTlsPurposeContext that)) {
            return false;
        }
        return purpose == that.purpose
                && Objects.equals(advertisedListenerName, that.advertisedListenerName)
                && Objects.equals(host, that.host)
                && Objects.equals(usageIdentifier, that.usageIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(purpose, advertisedListenerName, host, usageIdentifier);
    }

    @Override
    public String toString() {
        return "ServerTlsPurposeContext{" + purpose
                + (advertisedListenerName != null ? ", listener=" + advertisedListenerName : "")
                + (host != null ? ", host=" + host : "")
                + (usageIdentifier != null ? ", " + usageIdentifier : "") + "}";
    }
}
