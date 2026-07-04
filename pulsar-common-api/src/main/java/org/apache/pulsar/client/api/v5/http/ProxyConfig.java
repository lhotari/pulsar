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
package org.apache.pulsar.client.api.v5.http;

import java.util.Optional;

/**
 * Proxy settings for a {@link PulsarHttpClient} (PIP-478).
 *
 * @param type     the proxy protocol
 * @param host     the proxy host
 * @param port     the proxy port
 * @param username an optional username for proxy authentication
 * @param password an optional password for proxy authentication
 */
public record ProxyConfig(Type type, String host, int port, String username, String password) {

    /** The proxy protocol. */
    public enum Type {
        HTTP, SOCKS5
    }

    /**
     * Create an unauthenticated proxy config.
     *
     * @param type the proxy protocol
     * @param host the proxy host
     * @param port the proxy port
     * @return a new {@link ProxyConfig}
     */
    public static ProxyConfig of(Type type, String host, int port) {
        return new ProxyConfig(type, host, port, null, null);
    }

    /**
     * @return the proxy username, if authentication is configured
     */
    public Optional<String> usernameOptional() {
        return Optional.ofNullable(username);
    }

    /**
     * @return the proxy password, if authentication is configured
     */
    public Optional<String> passwordOptional() {
        return Optional.ofNullable(password);
    }
}
