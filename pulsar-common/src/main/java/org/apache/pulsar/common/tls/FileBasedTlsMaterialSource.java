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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Immutable, user-facing configuration describing a single file (or keystore) based source of TLS
 * material.
 *
 * <p>This is the configuration object that users register with a {@link FileBasedTlsMaterialProvider}
 * via {@link FileBasedTlsMaterialProvider#registerSource(TlsPurposeContext, FileBasedTlsMaterialSource)}.
 * It supports both PEM material (separate cert/key/trust-cert files) and Java keystores
 * (PKCS12/JKS). All fields are optional so that the same object can describe either a client-side or
 * a server-side source; the provider decides how to interpret it based on the {@link TlsPurposeContext}
 * it is registered against.
 *
 * <p>Instances are created through the {@link Builder} obtained from {@link #builder()} and are
 * immutable and thread-safe once built.
 */
@EqualsAndHashCode
@ToString
public final class FileBasedTlsMaterialSource {

    // PEM material
    private final String trustCertsFilePath;
    private final String certificateFilePath;
    private final String keyFilePath;

    // Keystore (key material)
    private final String keyStoreType;
    private final String keyStorePath;
    private final String keyStorePassword;

    // Truststore (trust material)
    private final String trustStoreType;
    private final String trustStorePath;
    private final String trustStorePassword;

    // Handshake tuning
    private final Set<String> tlsCiphers;
    private final Set<String> tlsProtocols;

    // Behavioural flags
    private final boolean allowInsecureConnection;
    private final boolean hostnameVerificationEnabled;
    private final boolean requireTrustedClientCertOnConnect;

    // Optional security provider name (e.g. an explicit JCE provider)
    private final String tlsProvider;

    private FileBasedTlsMaterialSource(Builder builder) {
        this.trustCertsFilePath = builder.trustCertsFilePath;
        this.certificateFilePath = builder.certificateFilePath;
        this.keyFilePath = builder.keyFilePath;
        this.keyStoreType = builder.keyStoreType;
        this.keyStorePath = builder.keyStorePath;
        this.keyStorePassword = builder.keyStorePassword;
        this.trustStoreType = builder.trustStoreType;
        this.trustStorePath = builder.trustStorePath;
        this.trustStorePassword = builder.trustStorePassword;
        this.tlsCiphers = builder.tlsCiphers == null ? Collections.emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(builder.tlsCiphers));
        this.tlsProtocols = builder.tlsProtocols == null ? Collections.emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(builder.tlsProtocols));
        this.allowInsecureConnection = builder.allowInsecureConnection;
        this.hostnameVerificationEnabled = builder.hostnameVerificationEnabled;
        this.requireTrustedClientCertOnConnect = builder.requireTrustedClientCertOnConnect;
        this.tlsProvider = builder.tlsProvider;
    }

    /**
     * Create a new builder.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The path to the PEM file containing the trust certificates, or {@code null}.
     *
     * @return the trust certificates file path
     */
    public String getTrustCertsFilePath() {
        return trustCertsFilePath;
    }

    /**
     * The path to the PEM file containing the certificate chain, or {@code null}.
     *
     * @return the certificate file path
     */
    public String getCertificateFilePath() {
        return certificateFilePath;
    }

    /**
     * The path to the PEM file containing the PKCS#8 private key, or {@code null}.
     *
     * @return the key file path
     */
    public String getKeyFilePath() {
        return keyFilePath;
    }

    /**
     * The type of the key store (for example {@code PKCS12} or {@code JKS}), or {@code null}.
     *
     * @return the key store type
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * The path to the key store, or {@code null}.
     *
     * @return the key store path
     */
    public String getKeyStorePath() {
        return keyStorePath;
    }

    /**
     * The key store password, or {@code null}.
     *
     * @return the key store password
     */
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    /**
     * The type of the trust store (for example {@code PKCS12} or {@code JKS}), or {@code null}.
     *
     * @return the trust store type
     */
    public String getTrustStoreType() {
        return trustStoreType;
    }

    /**
     * The path to the trust store, or {@code null}.
     *
     * @return the trust store path
     */
    public String getTrustStorePath() {
        return trustStorePath;
    }

    /**
     * The trust store password, or {@code null}.
     *
     * @return the trust store password
     */
    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    /**
     * The set of enabled TLS ciphers. Empty means use the platform defaults.
     *
     * @return the TLS ciphers
     */
    public Set<String> getTlsCiphers() {
        return tlsCiphers;
    }

    /**
     * The set of enabled TLS protocols. Empty means use the platform defaults.
     *
     * @return the TLS protocols
     */
    public Set<String> getTlsProtocols() {
        return tlsProtocols;
    }

    /**
     * Whether insecure connections are allowed (no trust certificate verification).
     *
     * @return {@code true} if insecure connections are allowed
     */
    public boolean isAllowInsecureConnection() {
        return allowInsecureConnection;
    }

    /**
     * Whether hostname verification is enabled. Only relevant for client-side usage.
     *
     * @return {@code true} if hostname verification is enabled
     */
    public boolean isHostnameVerificationEnabled() {
        return hostnameVerificationEnabled;
    }

    /**
     * Whether a trusted client certificate is required on connect (mTLS). Only relevant for
     * server-side usage.
     *
     * @return {@code true} if a trusted client certificate is required
     */
    public boolean isRequireTrustedClientCertOnConnect() {
        return requireTrustedClientCertOnConnect;
    }

    /**
     * The optional security provider name, or {@code null}.
     *
     * @return the TLS provider name
     */
    public String getTlsProvider() {
        return tlsProvider;
    }

    /**
     * Builder for {@link FileBasedTlsMaterialSource}.
     */
    public static final class Builder {
        private String trustCertsFilePath;
        private String certificateFilePath;
        private String keyFilePath;
        private String keyStoreType;
        private String keyStorePath;
        private String keyStorePassword;
        private String trustStoreType;
        private String trustStorePath;
        private String trustStorePassword;
        private Set<String> tlsCiphers;
        private Set<String> tlsProtocols;
        private boolean allowInsecureConnection = false;
        private boolean hostnameVerificationEnabled = true;
        private boolean requireTrustedClientCertOnConnect = false;
        private String tlsProvider;

        private Builder() {
        }

        /**
         * Set the PEM trust certificates file path.
         *
         * @param trustCertsFilePath the trust certificates file path
         * @return this builder
         */
        public Builder trustCertsFilePath(String trustCertsFilePath) {
            this.trustCertsFilePath = trustCertsFilePath;
            return this;
        }

        /**
         * Set the PEM certificate chain file path.
         *
         * @param certificateFilePath the certificate file path
         * @return this builder
         */
        public Builder certificateFilePath(String certificateFilePath) {
            this.certificateFilePath = certificateFilePath;
            return this;
        }

        /**
         * Set the PEM PKCS#8 private key file path.
         *
         * @param keyFilePath the key file path
         * @return this builder
         */
        public Builder keyFilePath(String keyFilePath) {
            this.keyFilePath = keyFilePath;
            return this;
        }

        /**
         * Set the key store type (for example {@code PKCS12} or {@code JKS}).
         *
         * @param keyStoreType the key store type
         * @return this builder
         */
        public Builder keyStoreType(String keyStoreType) {
            this.keyStoreType = keyStoreType;
            return this;
        }

        /**
         * Set the key store path.
         *
         * @param keyStorePath the key store path
         * @return this builder
         */
        public Builder keyStorePath(String keyStorePath) {
            this.keyStorePath = keyStorePath;
            return this;
        }

        /**
         * Set the key store password.
         *
         * @param keyStorePassword the key store password
         * @return this builder
         */
        public Builder keyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        /**
         * Set the trust store type (for example {@code PKCS12} or {@code JKS}).
         *
         * @param trustStoreType the trust store type
         * @return this builder
         */
        public Builder trustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        /**
         * Set the trust store path.
         *
         * @param trustStorePath the trust store path
         * @return this builder
         */
        public Builder trustStorePath(String trustStorePath) {
            this.trustStorePath = trustStorePath;
            return this;
        }

        /**
         * Set the trust store password.
         *
         * @param trustStorePassword the trust store password
         * @return this builder
         */
        public Builder trustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        /**
         * Set the enabled TLS ciphers.
         *
         * @param tlsCiphers the TLS ciphers
         * @return this builder
         */
        public Builder tlsCiphers(Set<String> tlsCiphers) {
            this.tlsCiphers = tlsCiphers;
            return this;
        }

        /**
         * Set the enabled TLS protocols.
         *
         * @param tlsProtocols the TLS protocols
         * @return this builder
         */
        public Builder tlsProtocols(Set<String> tlsProtocols) {
            this.tlsProtocols = tlsProtocols;
            return this;
        }

        /**
         * Set whether insecure connections are allowed.
         *
         * @param allowInsecureConnection whether insecure connections are allowed
         * @return this builder
         */
        public Builder allowInsecureConnection(boolean allowInsecureConnection) {
            this.allowInsecureConnection = allowInsecureConnection;
            return this;
        }

        /**
         * Set whether hostname verification is enabled (client only).
         *
         * @param hostnameVerificationEnabled whether hostname verification is enabled
         * @return this builder
         */
        public Builder hostnameVerificationEnabled(boolean hostnameVerificationEnabled) {
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            return this;
        }

        /**
         * Set whether a trusted client certificate is required on connect (server only).
         *
         * @param requireTrustedClientCertOnConnect whether a trusted client certificate is required
         * @return this builder
         */
        public Builder requireTrustedClientCertOnConnect(boolean requireTrustedClientCertOnConnect) {
            this.requireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
            return this;
        }

        /**
         * Set the optional security provider name.
         *
         * @param tlsProvider the TLS provider name
         * @return this builder
         */
        public Builder tlsProvider(String tlsProvider) {
            this.tlsProvider = tlsProvider;
            return this;
        }

        /**
         * Build the {@link FileBasedTlsMaterialSource}.
         *
         * @return a new immutable instance
         */
        public FileBasedTlsMaterialSource build() {
            return new FileBasedTlsMaterialSource(this);
        }
    }
}
