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
package org.apache.pulsar.common.tls.impl;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.netty.handler.ssl.SslContext;
import java.util.List;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;

/**
 * A client Netty {@link SslContext} that bakes HTTPS endpoint identification (hostname verification) into
 * every {@link SSLEngine} it produces, wrapping a framework-synthesized JDK-backed context (PIP-478).
 *
 * <p>The universal {@code javax.net.ssl.SSLContext} fallback the framework synthesizes a Netty client
 * context from is always JDK-backed — a {@link io.netty.handler.ssl.JdkSslContext} wrapping the
 * factory-supplied {@code SSLContext}. A JDK {@code SSLContext} cannot carry an
 * {@code endpointIdentificationAlgorithm}: that is an engine-level {@link SSLParameters} setting, not a
 * context property. So when the consumer's TLS policy enables hostname verification but the factory could
 * only supply an {@code SSLContext} (returning {@code empty()} for the Netty class), the framework applies
 * the algorithm per engine here.
 *
 * <p>This is the faithful equivalent of the native {@code FileBasedTlsFactory} path, which bakes
 * {@code "HTTPS"} into the context via {@code SslContextBuilder.endpointIdentificationAlgorithm(...)} — the
 * consumers rely on the context to carry verification and never re-apply it per connection. It is sound
 * because the synthesized context is always the JDK backend, on which a per-{@code SSLEngine} override
 * cleanly forces verification on (unlike Netty's OpenSSL backend, which fixes the algorithm at build time);
 * see PIP-478 Detailed Design, "client-side hostname verification". Netty's own {@code JdkSslContext}
 * applies the algorithm through the same {@link SSLParameters} round-trip when a build-time algorithm is
 * configured, so re-applying it after {@code newEngine(...)} preserves the SNI / protocol / cipher
 * configuration the delegate already set.
 */
final class HostnameVerifyingSslContext extends SslContext {

    private final SslContext delegate;

    HostnameVerifyingSslContext(SslContext delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isClient() {
        return delegate.isClient();
    }

    @Override
    public List<String> cipherSuites() {
        return delegate.cipherSuites();
    }

    @Override
    @SuppressWarnings("deprecation") // ApplicationProtocolNegotiator is a deprecated but still-abstract SPI type
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return delegate.applicationProtocolNegotiator();
    }

    @Override
    public SSLSessionContext sessionContext() {
        return delegate.sessionContext();
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator alloc) {
        return enableHttpsEndpointIdentification(delegate.newEngine(alloc));
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator alloc, String peerHost, int peerPort) {
        return enableHttpsEndpointIdentification(delegate.newEngine(alloc, peerHost, peerPort));
    }

    private static SSLEngine enableHttpsEndpointIdentification(SSLEngine engine) {
        // getSSLParameters() returns a snapshot of the engine's full configuration (protocols, ciphers,
        // SNI server names the delegate already set); we flip only the endpoint-identification algorithm
        // and write the snapshot back, leaving everything else untouched.
        SSLParameters parameters = engine.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(parameters);
        return engine;
    }
}
