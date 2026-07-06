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
package org.apache.pulsar.tls;

import java.util.Objects;
import java.util.Optional;

/**
 * Identifies <em>why</em> TLS is requested and in what role (PIP-478).
 *
 * <p>A {@code TlsPurpose} is a <strong>simple named key</strong>, not a type hierarchy: a
 * {@link Role} (client or server), an open {@link #name()}, and an optional single-level
 * {@link #fallback()} naming the purpose to consult when this one has no material configured.
 * A {@link PulsarTlsFactory} serves distinct TLS material per purpose; the well-known purposes are
 * exposed as constants, and components may mint additional open-named purposes with
 * {@link #client(String, TlsPurpose)} / {@link #server(String, TlsPurpose)} (or the fallback-less
 * {@link #client(String)} / {@link #server(String)}).
 *
 * <p><b>Empty-fallback semantics.</b> When {@link #fallback()} is empty and nothing is configured
 * for this purpose, resolution ends: for the {@link Role#CLIENT} role it resolves to the
 * <em>system default</em> (the OS trust store, no client certificate); for the {@link Role#SERVER}
 * role it is a configuration error. This is why the OAuth2 / identity-provider purpose
 * ({@link #CLIENT_OAUTH2}) has no fallback — its calls must NOT silently reuse Pulsar-cluster TLS
 * material, since the identity provider is a different trust domain.
 *
 * <p>The value is immutable; the fallback is fixed at construction. Instances are safe to use as map
 * keys — {@link #equals(Object)} / {@link #hashCode()} are defined over the role and name only. The
 * {@link #fallback()} is <em>resolution metadata</em>, not part of the key identity: a purpose is
 * uniquely named, and each name is expected to be minted once with a single fallback, so two instances
 * with the same {@code (role, name)} are the same configuration entry regardless of fallback.
 */
public final class TlsPurpose {

    /** Whether the purpose describes an outbound (client) or inbound (server) TLS endpoint. */
    public enum Role {
        /** Outbound TLS: the local endpoint acts as a TLS client. */
        CLIENT,
        /** Inbound TLS: the local endpoint acts as a TLS server. */
        SERVER
    }

    // Well-known CLIENT purposes.

    /** Pulsar-cluster traffic: binary protocol, HTTP topic lookup, and the admin client. */
    public static final TlsPurpose CLIENT_DEFAULT = new TlsPurpose(Role.CLIENT, "default", null);

    /**
     * OAuth2 / identity-provider calls. Its empty fallback resolves to the system default rather than
     * to the cluster material, because the identity provider is a different trust domain and its TLS
     * material must not be shared with Pulsar-cluster connections.
     */
    public static final TlsPurpose CLIENT_OAUTH2 = new TlsPurpose(Role.CLIENT, "oauth2", null);

    /**
     * A server component's own outbound Pulsar-client traffic — geo-replication, proxy&rarr;broker,
     * websocket&rarr;broker, and functions-worker&rarr;broker connections. A distinct trust domain
     * from both the server listeners and any application client (configured through the dedicated
     * {@code brokerClient*} keys on the server side).
     */
    public static final TlsPurpose BROKER_CLIENT = new TlsPurpose(Role.CLIENT, "broker-client", null);

    // Well-known SERVER purposes.

    /** The broker's binary protocol listener(s). */
    public static final TlsPurpose BROKER = new TlsPurpose(Role.SERVER, "broker", null);

    /** The proxy's binary protocol front-end. */
    public static final TlsPurpose PROXY = new TlsPurpose(Role.SERVER, "proxy", null);

    /** A component's Jetty web service (broker / proxy / functions-worker). */
    public static final TlsPurpose WEB = new TlsPurpose(Role.SERVER, "web", null);

    private final Role role;
    private final String name;
    private final TlsPurpose fallback;

    private TlsPurpose(Role role, String name, TlsPurpose fallback) {
        this.role = Objects.requireNonNull(role, "role must not be null");
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.fallback = fallback;
    }

    /**
     * @return whether this purpose is a client-role (outbound) or server-role (inbound) endpoint
     */
    public Role role() {
        return role;
    }

    /**
     * @return the well-known or plugin-minted name, e.g. {@code "default"}, {@code "oauth2"},
     *         {@code "broker-client"}, {@code "broker"}
     */
    public String name() {
        return name;
    }

    /**
     * The purpose consulted when no material is configured for this one (single level, fixed at
     * construction). An empty value means: {@link Role#CLIENT} role &rarr; the system default (OS
     * trust store, no client certificate); {@link Role#SERVER} role &rarr; a configuration error.
     *
     * <p>The fallback is <em>resolution metadata</em>: it steers how an unconfigured purpose resolves, but
     * it is deliberately excluded from {@link #equals(Object)} / {@link #hashCode()}, so it never affects
     * this purpose's identity as a map key.
     *
     * @return the fallback purpose, or empty when this purpose has no fallback
     */
    public Optional<TlsPurpose> fallback() {
        return Optional.ofNullable(fallback);
    }

    /**
     * Mint a client-role purpose with no fallback (empty fallback resolves to the system default).
     *
     * @param name the open purpose name
     * @return a new client-role {@link TlsPurpose}
     */
    public static TlsPurpose client(String name) {
        return new TlsPurpose(Role.CLIENT, name, null);
    }

    /**
     * Mint a client-role purpose with an explicit single-level fallback, e.g.
     * {@code TlsPurpose.client("oauth2.myPlugin", CLIENT_OAUTH2)}.
     *
     * @param name     the open purpose name
     * @param fallback the purpose to consult when this one has no material configured
     * @return a new client-role {@link TlsPurpose}
     */
    public static TlsPurpose client(String name, TlsPurpose fallback) {
        return new TlsPurpose(Role.CLIENT, name, Objects.requireNonNull(fallback, "fallback must not be null"));
    }

    /**
     * Mint a server-role purpose with no fallback (empty fallback is a configuration error when
     * nothing is configured).
     *
     * @param name the open purpose name
     * @return a new server-role {@link TlsPurpose}
     */
    public static TlsPurpose server(String name) {
        return new TlsPurpose(Role.SERVER, name, null);
    }

    /**
     * Mint a server-role purpose with an explicit single-level fallback, e.g. an internal listener that
     * falls back to the main {@link #BROKER} material — {@code TlsPurpose.server("broker.internal", BROKER)}.
     *
     * @param name     the open purpose name
     * @param fallback the purpose to consult when this one has no material configured
     * @return a new server-role {@link TlsPurpose}
     */
    public static TlsPurpose server(String name, TlsPurpose fallback) {
        return new TlsPurpose(Role.SERVER, name, Objects.requireNonNull(fallback, "fallback must not be null"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TlsPurpose that)) {
            return false;
        }
        // Identity is (role, name) only. The fallback is resolution metadata — NOT part of the key — so
        // client("x") and client("x", CLIENT_DEFAULT) are the same configuration entry and resolve to the
        // same purpose→policy map slot; including the fallback here would split one config key into two and
        // cause silent lookup misses.
        return role == that.role && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(role, name);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TlsPurpose{").append(role).append(' ').append(name);
        if (fallback != null) {
            sb.append(" -> ").append(fallback.role).append(' ').append(fallback.name);
        }
        return sb.append('}').toString();
    }
}
