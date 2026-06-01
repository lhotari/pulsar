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

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Per-instance configuration for a {@link PulsarHttpClient} (PIP-478).
 *
 * <p>A plugin describes <em>what kind</em> of HTTP client it needs; the framework constructs, pools
 * and closes the instance. This deliberately does NOT carry cert/key/trust material directly — TLS
 * material lives in the configured {@code PulsarTlsMaterialProvider} and is looked up by
 * {@link #tlsPurpose()} (plus an optional {@link UsageIdentifier}).
 */
public final class PulsarHttpClientConfig {

    /**
     * Identifies a specific TLS usage within a well-known {@link ClientHttpTlsPurpose}, so a
     * deployment can pin distinct material for that one usage (resolution otherwise falls back
     * through the purpose chain). Example: the OAuth2 plugin's IdP client uses
     * {@code GENERIC} with {@code new UsageIdentifier(AuthenticationOAuth2.class, "idpHttpClient")}.
     *
     * @param owner      the owning class
     * @param identifier a stable name unique within the owner
     */
    public record UsageIdentifier(Class<?> owner, String identifier) {
    }

    private final ClientHttpTlsPurpose tlsPurpose;
    private final UsageIdentifier usageIdentifier;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final Duration requestTimeout;
    private final ProxyConfig proxy;
    private final String userAgent;
    private final Map<String, String> defaultHeaders;
    private final long maxResponseBodyBytes;
    private final boolean allowInsecureConnection;
    private final boolean hostnameVerificationEnabled;

    private PulsarHttpClientConfig(Builder b) {
        this.tlsPurpose = b.tlsPurpose;
        this.usageIdentifier = b.usageIdentifier;
        this.connectTimeout = b.connectTimeout;
        this.readTimeout = b.readTimeout;
        this.requestTimeout = b.requestTimeout;
        this.proxy = b.proxy;
        this.userAgent = b.userAgent;
        this.defaultHeaders = Collections.unmodifiableMap(new LinkedHashMap<>(b.defaultHeaders));
        this.maxResponseBodyBytes = b.maxResponseBodyBytes;
        this.allowInsecureConnection = b.allowInsecureConnection;
        this.hostnameVerificationEnabled = b.hostnameVerificationEnabled;
    }

    /**
     * @return the TLS purpose used to resolve client TLS material from the material provider
     */
    public ClientHttpTlsPurpose tlsPurpose() {
        return tlsPurpose;
    }

    /**
     * @return a finer-grained usage identifier within the purpose, if any
     */
    public Optional<UsageIdentifier> usageIdentifier() {
        return Optional.ofNullable(usageIdentifier);
    }

    /**
     * @return the connection timeout
     */
    public Duration connectTimeout() {
        return connectTimeout;
    }

    /**
     * @return the read (socket) timeout
     */
    public Duration readTimeout() {
        return readTimeout;
    }

    /**
     * @return the overall request timeout
     */
    public Duration requestTimeout() {
        return requestTimeout;
    }

    /**
     * @return the proxy settings, if any
     */
    public Optional<ProxyConfig> proxy() {
        return Optional.ofNullable(proxy);
    }

    /**
     * @return the {@code User-Agent} header value to send
     */
    public String userAgent() {
        return userAgent;
    }

    /**
     * @return headers attached to every request issued by this client
     */
    public Map<String, String> defaultHeaders() {
        return defaultHeaders;
    }

    /**
     * @return the maximum buffered response body size in bytes
     */
    public long maxResponseBodyBytes() {
        return maxResponseBodyBytes;
    }

    /**
     * @return whether to accept untrusted server certificates (insecure; logged once at WARN)
     */
    public boolean allowInsecureConnection() {
        return allowInsecureConnection;
    }

    /**
     * @return whether server hostname verification is enabled
     */
    public boolean hostnameVerificationEnabled() {
        return hostnameVerificationEnabled;
    }

    /**
     * Create a builder for the given TLS purpose.
     *
     * @param tlsPurpose the TLS purpose
     * @return a new {@link Builder}
     */
    public static Builder builder(ClientHttpTlsPurpose tlsPurpose) {
        return new Builder(tlsPurpose);
    }

    /**
     * Builder for {@link PulsarHttpClientConfig}.
     */
    public static final class Builder {
        private static final long DEFAULT_MAX_RESPONSE_BODY_BYTES = 16L * 1024 * 1024;

        private ClientHttpTlsPurpose tlsPurpose;
        private UsageIdentifier usageIdentifier;
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration readTimeout = Duration.ofSeconds(30);
        private Duration requestTimeout = Duration.ofSeconds(30);
        private ProxyConfig proxy;
        private String userAgent = "Pulsar-Java-v5";
        private final Map<String, String> defaultHeaders = new LinkedHashMap<>();
        private long maxResponseBodyBytes = DEFAULT_MAX_RESPONSE_BODY_BYTES;
        private boolean allowInsecureConnection = false;
        private boolean hostnameVerificationEnabled = true;

        private Builder(ClientHttpTlsPurpose tlsPurpose) {
            this.tlsPurpose = tlsPurpose;
        }

        /**
         * @param usageIdentifier a finer-grained TLS usage identifier within the purpose
         * @return this builder
         */
        public Builder usageIdentifier(UsageIdentifier usageIdentifier) {
            this.usageIdentifier = usageIdentifier;
            return this;
        }

        /**
         * @param connectTimeout the connection timeout
         * @return this builder
         */
        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * @param readTimeout the read timeout
         * @return this builder
         */
        public Builder readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        /**
         * @param requestTimeout the overall request timeout
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * @param proxy the proxy settings
         * @return this builder
         */
        public Builder proxy(ProxyConfig proxy) {
            this.proxy = proxy;
            return this;
        }

        /**
         * @param userAgent the {@code User-Agent} value
         * @return this builder
         */
        public Builder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        /**
         * @param name  default header name
         * @param value default header value
         * @return this builder
         */
        public Builder defaultHeader(String name, String value) {
            this.defaultHeaders.put(name, value);
            return this;
        }

        /**
         * @param maxResponseBodyBytes the maximum buffered response body size in bytes
         * @return this builder
         */
        public Builder maxResponseBodyBytes(long maxResponseBodyBytes) {
            this.maxResponseBodyBytes = maxResponseBodyBytes;
            return this;
        }

        /**
         * @param allowInsecureConnection whether to accept untrusted server certificates
         * @return this builder
         */
        public Builder allowInsecureConnection(boolean allowInsecureConnection) {
            this.allowInsecureConnection = allowInsecureConnection;
            return this;
        }

        /**
         * @param hostnameVerificationEnabled whether to verify the server hostname
         * @return this builder
         */
        public Builder hostnameVerificationEnabled(boolean hostnameVerificationEnabled) {
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            return this;
        }

        /**
         * @return a new immutable {@link PulsarHttpClientConfig}
         */
        public PulsarHttpClientConfig build() {
            return new PulsarHttpClientConfig(this);
        }
    }
}
