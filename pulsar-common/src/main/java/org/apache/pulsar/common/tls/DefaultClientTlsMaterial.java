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
 * Immutable default {@link ClientTlsMaterial} (PIP-478). Construct via {@link #builder()}.
 *
 * <p>Equality is value-based over all material and flags so the framework can cache the derived
 * {@code SSLContext} and rebuild only when the material actually changes.
 */
public final class DefaultClientTlsMaterial implements ClientTlsMaterial {

    private final PrivateKey privateKey;
    private final List<X509Certificate> keyCertChain;
    private final List<X509Certificate> trustCerts;
    private final List<String> tlsProtocols;
    private final List<String> tlsCiphers;
    private final boolean hostnameVerificationRequired;
    private final boolean trustAnyCaCert;

    private DefaultClientTlsMaterial(Builder b) {
        this.privateKey = b.privateKey;
        this.keyCertChain = immutable(b.keyCertChain);
        this.trustCerts = immutable(b.trustCerts);
        this.tlsProtocols = immutable(b.tlsProtocols);
        this.tlsCiphers = immutable(b.tlsCiphers);
        this.hostnameVerificationRequired = b.hostnameVerificationRequired;
        this.trustAnyCaCert = b.trustAnyCaCert;
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
    public boolean isHostnameVerificationRequired() {
        return hostnameVerificationRequired;
    }

    @Override
    public boolean isTrustAnyCaCert() {
        return trustAnyCaCert;
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
        if (!(o instanceof DefaultClientTlsMaterial that)) {
            return false;
        }
        return hostnameVerificationRequired == that.hostnameVerificationRequired
                && trustAnyCaCert == that.trustAnyCaCert
                && Objects.equals(privateKey, that.privateKey)
                && keyCertChain.equals(that.keyCertChain)
                && trustCerts.equals(that.trustCerts)
                && tlsProtocols.equals(that.tlsProtocols)
                && tlsCiphers.equals(that.tlsCiphers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(privateKey, keyCertChain, trustCerts, tlsProtocols, tlsCiphers,
                hostnameVerificationRequired, trustAnyCaCert);
    }

    /**
     * Builder for {@link DefaultClientTlsMaterial}.
     */
    public static final class Builder {
        private PrivateKey privateKey;
        private List<X509Certificate> keyCertChain;
        private List<X509Certificate> trustCerts;
        private List<String> tlsProtocols;
        private List<String> tlsCiphers;
        private boolean hostnameVerificationRequired = true;
        private boolean trustAnyCaCert = false;

        private Builder() {
        }

        /** @param privateKey the client private key (mTLS); @return this builder */
        public Builder privateKey(PrivateKey privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        /** @param keyCertChain the client certificate chain (mTLS); @return this builder */
        public Builder keyCertChain(List<X509Certificate> keyCertChain) {
            this.keyCertChain = keyCertChain;
            return this;
        }

        /** @param trustCerts the trusted CA certificates; @return this builder */
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

        /** @param hostnameVerificationRequired whether to verify the server hostname; @return this builder */
        public Builder hostnameVerificationRequired(boolean hostnameVerificationRequired) {
            this.hostnameVerificationRequired = hostnameVerificationRequired;
            return this;
        }

        /** @param trustAnyCaCert whether to trust any CA (insecure); @return this builder */
        public Builder trustAnyCaCert(boolean trustAnyCaCert) {
            this.trustAnyCaCert = trustAnyCaCert;
            return this;
        }

        /** @return a new immutable {@link DefaultClientTlsMaterial} */
        public DefaultClientTlsMaterial build() {
            return new DefaultClientTlsMaterial(this);
        }
    }
}
