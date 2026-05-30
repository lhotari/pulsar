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

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable default {@link ServerTlsMaterial} (PIP-478). Construct via {@link #builder()}.
 *
 * <p>Equality is value-based over all material and flags so the framework can cache the derived
 * {@code SSLContext} and rebuild only when the material actually changes.
 */
public final class DefaultServerTlsMaterial implements ServerTlsMaterial {

    private final PrivateKey privateKey;
    private final List<X509Certificate> keyCertChain;
    private final List<X509Certificate> trustCerts;
    private final List<String> tlsProtocols;
    private final List<String> tlsCiphers;
    private final boolean trustedClientCertRequired;
    private final boolean trustAnyClientCert;

    private DefaultServerTlsMaterial(Builder b) {
        this.privateKey = b.privateKey;
        this.keyCertChain = immutable(b.keyCertChain);
        this.trustCerts = immutable(b.trustCerts);
        this.tlsProtocols = immutable(b.tlsProtocols);
        this.tlsCiphers = immutable(b.tlsCiphers);
        this.trustedClientCertRequired = b.trustedClientCertRequired;
        this.trustAnyClientCert = b.trustAnyClientCert;
    }

    private static <T> List<T> immutable(List<T> in) {
        return in == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(in));
    }

    @Override
    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    @Override
    public List<X509Certificate> getKeyCertChain() {
        return keyCertChain;
    }

    @Override
    public List<X509Certificate> getTrustCerts() {
        return trustCerts;
    }

    @Override
    public List<String> getTlsProtocols() {
        return tlsProtocols;
    }

    @Override
    public List<String> getTlsCiphers() {
        return tlsCiphers;
    }

    @Override
    public boolean isTrustedClientCertRequired() {
        return trustedClientCertRequired;
    }

    @Override
    public boolean isTrustAnyClientCert() {
        return trustAnyClientCert;
    }

    /**
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DefaultServerTlsMaterial that)) {
            return false;
        }
        return trustedClientCertRequired == that.trustedClientCertRequired
                && trustAnyClientCert == that.trustAnyClientCert
                && Objects.equals(privateKey, that.privateKey)
                && keyCertChain.equals(that.keyCertChain)
                && trustCerts.equals(that.trustCerts)
                && tlsProtocols.equals(that.tlsProtocols)
                && tlsCiphers.equals(that.tlsCiphers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privateKey, keyCertChain, trustCerts, tlsProtocols, tlsCiphers,
                trustedClientCertRequired, trustAnyClientCert);
    }

    /**
     * Builder for {@link DefaultServerTlsMaterial}.
     */
    public static final class Builder {
        private PrivateKey privateKey;
        private List<X509Certificate> keyCertChain;
        private List<X509Certificate> trustCerts;
        private List<String> tlsProtocols;
        private List<String> tlsCiphers;
        private boolean trustedClientCertRequired = false;
        private boolean trustAnyClientCert = false;

        private Builder() {
        }

        /** @param privateKey the server private key; @return this builder */
        public Builder privateKey(PrivateKey privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        /** @param keyCertChain the server certificate chain; @return this builder */
        public Builder keyCertChain(List<X509Certificate> keyCertChain) {
            this.keyCertChain = keyCertChain;
            return this;
        }

        /** @param trustCerts the trusted client-CA certificates; @return this builder */
        public Builder trustCerts(List<X509Certificate> trustCerts) {
            this.trustCerts = trustCerts;
            return this;
        }

        /** @param tlsProtocols enabled TLS protocols; @return this builder */
        public Builder tlsProtocols(List<String> tlsProtocols) {
            this.tlsProtocols = tlsProtocols;
            return this;
        }

        /** @param tlsCiphers enabled TLS cipher suites; @return this builder */
        public Builder tlsCiphers(List<String> tlsCiphers) {
            this.tlsCiphers = tlsCiphers;
            return this;
        }

        /** @param trustedClientCertRequired whether a trusted client cert is required (mTLS); @return this builder */
        public Builder trustedClientCertRequired(boolean trustedClientCertRequired) {
            this.trustedClientCertRequired = trustedClientCertRequired;
            return this;
        }

        /** @param trustAnyClientCert whether to accept untrusted client certs (insecure); @return this builder */
        public Builder trustAnyClientCert(boolean trustAnyClientCert) {
            this.trustAnyClientCert = trustAnyClientCert;
            return this;
        }

        /** @return a new immutable {@link DefaultServerTlsMaterial} */
        public DefaultServerTlsMaterial build() {
            return new DefaultServerTlsMaterial(this);
        }
    }
}
