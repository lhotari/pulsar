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

import java.util.List;
import java.util.Objects;

/**
 * The single, user-facing TLS configuration value: a flat, immutable description of <em>what</em>
 * material to use and the policy flags (PIP-478).
 *
 * <p>This subsumes the experimental PIP-466 {@code org.apache.pulsar.client.api.v5.config.TlsPolicy}
 * (PEM-only) and lives in the neutral SPI module so the client builder and the server components
 * consume the same value. To keep it friendly to a future configuration-file loader it is a
 * <strong>flat value with a {@link Format} discriminator</strong> rather than a polymorphic hierarchy —
 * one type covering PEM and keystore/truststore, plus the common flags, with static factories for the
 * common shapes.
 *
 * <p>{@code TlsPolicy} describes material <em>locations</em>, not the loaded material, so it stays a
 * small serializable value; loading, caching, and rotation are the internal material source's job
 * inside the default {@code FileBasedTlsFactory}.
 */
public final class TlsPolicy {

    /** Whether the TLS material is described as PEM files or as a keystore/truststore pair. */
    public enum Format {
        /** PEM files: trust certs, certificate, and private key. */
        PEM,
        /** Keystore/truststore (JKS or PKCS12). */
        KEYSTORE
    }

    private final Format format;
    // format == PEM
    private final String trustCertsFilePath;
    private final String certificateFilePath;
    private final String keyFilePath;
    // format == KEYSTORE
    private final String trustStorePath;
    private final String trustStorePassword;
    private final String keyStorePath;
    private final String keyStorePassword;
    private final String keyStoreType;
    private final String trustStoreType;
    // common flags (both formats)
    private final boolean allowInsecureConnection;
    private final boolean enableHostnameVerification;
    private final List<String> protocols;
    private final List<String> ciphers;

    private TlsPolicy(Builder b) {
        this.format = b.format;
        this.trustCertsFilePath = b.trustCertsFilePath;
        this.certificateFilePath = b.certificateFilePath;
        this.keyFilePath = b.keyFilePath;
        this.trustStorePath = b.trustStorePath;
        this.trustStorePassword = b.trustStorePassword;
        this.keyStorePath = b.keyStorePath;
        this.keyStorePassword = b.keyStorePassword;
        this.keyStoreType = b.keyStoreType;
        this.trustStoreType = b.trustStoreType;
        this.allowInsecureConnection = b.allowInsecureConnection;
        this.enableHostnameVerification = b.enableHostnameVerification;
        this.protocols = List.copyOf(b.protocols);
        this.ciphers = List.copyOf(b.ciphers);
    }

    /**
     * @return the material format discriminator (PEM or keystore)
     */
    public Format format() {
        return format;
    }

    /**
     * @return the trusted CA certificate file path (PEM format), or {@code null}
     */
    public String trustCertsFilePath() {
        return trustCertsFilePath;
    }

    /**
     * @return the client certificate file path (PEM format), or {@code null}
     */
    public String certificateFilePath() {
        return certificateFilePath;
    }

    /**
     * @return the client private key file path (PEM format), or {@code null}
     */
    public String keyFilePath() {
        return keyFilePath;
    }

    /**
     * @return the truststore path (keystore format), or {@code null}
     */
    public String trustStorePath() {
        return trustStorePath;
    }

    /**
     * @return the truststore password (keystore format), or {@code null}
     */
    public String trustStorePassword() {
        return trustStorePassword;
    }

    /**
     * @return the keystore path (keystore format), or {@code null}
     */
    public String keyStorePath() {
        return keyStorePath;
    }

    /**
     * @return the keystore password (keystore format), or {@code null}
     */
    public String keyStorePassword() {
        return keyStorePassword;
    }

    /**
     * @return the keystore type (e.g. {@code JKS} / {@code PKCS12}); blank/{@code null} means the JDK
     *         {@link java.security.KeyStore#getDefaultType() default keystore type}
     */
    public String keyStoreType() {
        return keyStoreType;
    }

    /**
     * @return the truststore type (e.g. {@code JKS} / {@code PKCS12}); blank/{@code null} means the JDK
     *         {@link java.security.KeyStore#getDefaultType() default keystore type}
     */
    public String trustStoreType() {
        return trustStoreType;
    }

    /**
     * @return whether connecting to endpoints with untrusted certificates is allowed
     */
    public boolean allowInsecureConnection() {
        return allowInsecureConnection;
    }

    /**
     * @return whether the peer hostname is verified against the certificate
     */
    public boolean enableHostnameVerification() {
        return enableHostnameVerification;
    }

    /**
     * @return the enabled TLS protocols, or an empty list to use the defaults
     */
    public List<String> protocols() {
        return protocols;
    }

    /**
     * @return the enabled TLS cipher suites, or an empty list to use the defaults
     */
    public List<String> ciphers() {
        return ciphers;
    }

    /**
     * Create a PEM-format policy.
     *
     * @param trustCerts the trusted CA certificate file path (may be {@code null} for system default)
     * @param cert       the client certificate file path (may be {@code null} when not using mTLS)
     * @param key        the client private key file path (may be {@code null} when not using mTLS)
     * @return a new PEM-format {@link TlsPolicy}
     */
    public static TlsPolicy pem(String trustCerts, String cert, String key) {
        return builder()
                .format(Format.PEM)
                .trustCertsFilePath(trustCerts)
                .certificateFilePath(cert)
                .keyFilePath(key)
                .build();
    }

    /**
     * Create a keystore-format policy that uses a single store type for both the keystore and the
     * truststore (the common case). To use different types (e.g. a PKCS12 keystore with a JKS truststore),
     * build via {@link #builder()} and set {@link Builder#keyStoreType(String)} and
     * {@link Builder#trustStoreType(String)} separately.
     *
     * @param trustStore   the truststore path
     * @param trustStorePw the truststore password
     * @param keyStore     the keystore path (may be {@code null} when not using mTLS)
     * @param keyStorePw   the keystore password (may be {@code null} when not using mTLS)
     * @param storeType    the store type (e.g. {@code JKS} / {@code PKCS12}) applied to BOTH the keystore and
     *                     the truststore
     * @return a new keystore-format {@link TlsPolicy}
     */
    public static TlsPolicy keyStore(String trustStore, String trustStorePw,
                                     String keyStore, String keyStorePw, String storeType) {
        return builder()
                .format(Format.KEYSTORE)
                .trustStorePath(trustStore)
                .trustStorePassword(trustStorePw)
                .keyStorePath(keyStore)
                .keyStorePassword(keyStorePw)
                .keyStoreType(storeType)
                .trustStoreType(storeType)
                .build();
    }

    /**
     * Create an insecure PEM policy that accepts any certificate and skips hostname verification
     * (development only).
     *
     * @return a new insecure {@link TlsPolicy}
     */
    public static TlsPolicy insecure() {
        return builder()
                .allowInsecureConnection(true)
                .enableHostnameVerification(false)
                .build();
    }

    /**
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TlsPolicy that)) {
            return false;
        }
        return allowInsecureConnection == that.allowInsecureConnection
                && enableHostnameVerification == that.enableHostnameVerification
                && format == that.format
                && Objects.equals(trustCertsFilePath, that.trustCertsFilePath)
                && Objects.equals(certificateFilePath, that.certificateFilePath)
                && Objects.equals(keyFilePath, that.keyFilePath)
                && Objects.equals(trustStorePath, that.trustStorePath)
                && Objects.equals(trustStorePassword, that.trustStorePassword)
                && Objects.equals(keyStorePath, that.keyStorePath)
                && Objects.equals(keyStorePassword, that.keyStorePassword)
                && Objects.equals(keyStoreType, that.keyStoreType)
                && Objects.equals(trustStoreType, that.trustStoreType)
                && protocols.equals(that.protocols)
                && ciphers.equals(that.ciphers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, trustCertsFilePath, certificateFilePath, keyFilePath,
                trustStorePath, trustStorePassword, keyStorePath, keyStorePassword, keyStoreType,
                trustStoreType, allowInsecureConnection, enableHostnameVerification, protocols, ciphers);
    }

    @Override
    public String toString() {
        // Passwords are intentionally masked so the value can be logged without leaking secrets.
        return "TlsPolicy{format=" + format
                + ", trustCertsFilePath=" + trustCertsFilePath
                + ", certificateFilePath=" + certificateFilePath
                + ", keyFilePath=" + keyFilePath
                + ", trustStorePath=" + trustStorePath
                + ", trustStorePassword=" + (trustStorePassword == null ? "null" : "****")
                + ", keyStorePath=" + keyStorePath
                + ", keyStorePassword=" + (keyStorePassword == null ? "null" : "****")
                + ", keyStoreType=" + keyStoreType
                + ", trustStoreType=" + trustStoreType
                + ", allowInsecureConnection=" + allowInsecureConnection
                + ", enableHostnameVerification=" + enableHostnameVerification
                + ", protocols=" + protocols
                + ", ciphers=" + ciphers
                + '}';
    }

    /**
     * Builder for {@link TlsPolicy}. Defaults to {@link Format#PEM}, secure connections, and hostname
     * verification enabled.
     */
    public static final class Builder {
        private Format format = Format.PEM;
        private String trustCertsFilePath;
        private String certificateFilePath;
        private String keyFilePath;
        private String trustStorePath;
        private String trustStorePassword;
        private String keyStorePath;
        private String keyStorePassword;
        private String keyStoreType;
        private String trustStoreType;
        private boolean allowInsecureConnection = false;
        private boolean enableHostnameVerification = true;
        private List<String> protocols = List.of();
        private List<String> ciphers = List.of();

        private Builder() {
        }

        /**
         * @param format the material format discriminator
         * @return this builder
         */
        public Builder format(Format format) {
            this.format = Objects.requireNonNull(format, "format must not be null");
            return this;
        }

        /**
         * @param trustCertsFilePath the trusted CA certificate file path (PEM)
         * @return this builder
         */
        public Builder trustCertsFilePath(String trustCertsFilePath) {
            this.trustCertsFilePath = trustCertsFilePath;
            return this;
        }

        /**
         * @param certificateFilePath the client certificate file path (PEM)
         * @return this builder
         */
        public Builder certificateFilePath(String certificateFilePath) {
            this.certificateFilePath = certificateFilePath;
            return this;
        }

        /**
         * @param keyFilePath the client private key file path (PEM)
         * @return this builder
         */
        public Builder keyFilePath(String keyFilePath) {
            this.keyFilePath = keyFilePath;
            return this;
        }

        /**
         * @param trustStorePath the truststore path (keystore format)
         * @return this builder
         */
        public Builder trustStorePath(String trustStorePath) {
            this.trustStorePath = trustStorePath;
            return this;
        }

        /**
         * @param trustStorePassword the truststore password (keystore format)
         * @return this builder
         */
        public Builder trustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        /**
         * @param keyStorePath the keystore path (keystore format)
         * @return this builder
         */
        public Builder keyStorePath(String keyStorePath) {
            this.keyStorePath = keyStorePath;
            return this;
        }

        /**
         * @param keyStorePassword the keystore password (keystore format)
         * @return this builder
         */
        public Builder keyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        /**
         * @param keyStoreType the keystore type (e.g. {@code JKS} / {@code PKCS12}); blank/{@code null} uses
         *                     the JDK default keystore type
         * @return this builder
         */
        public Builder keyStoreType(String keyStoreType) {
            this.keyStoreType = keyStoreType;
            return this;
        }

        /**
         * @param trustStoreType the truststore type (e.g. {@code JKS} / {@code PKCS12}); blank/{@code null}
         *                       uses the JDK default keystore type
         * @return this builder
         */
        public Builder trustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        /**
         * @param allowInsecureConnection whether to accept untrusted certificates
         * @return this builder
         */
        public Builder allowInsecureConnection(boolean allowInsecureConnection) {
            this.allowInsecureConnection = allowInsecureConnection;
            return this;
        }

        /**
         * @param enableHostnameVerification whether to verify the peer hostname
         * @return this builder
         */
        public Builder enableHostnameVerification(boolean enableHostnameVerification) {
            this.enableHostnameVerification = enableHostnameVerification;
            return this;
        }

        /**
         * @param protocols the enabled TLS protocols
         * @return this builder
         */
        public Builder protocols(List<String> protocols) {
            this.protocols = protocols == null ? List.of() : List.copyOf(protocols);
            return this;
        }

        /**
         * @param ciphers the enabled TLS cipher suites
         * @return this builder
         */
        public Builder ciphers(List<String> ciphers) {
            this.ciphers = ciphers == null ? List.of() : List.copyOf(ciphers);
            return this;
        }

        /**
         * @return a new immutable {@link TlsPolicy}
         */
        public TlsPolicy build() {
            return new TlsPolicy(this);
        }
    }
}
