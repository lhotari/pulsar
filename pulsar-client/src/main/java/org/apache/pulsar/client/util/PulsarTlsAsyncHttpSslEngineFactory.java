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
package org.apache.pulsar.client.util;

import java.util.Collections;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.tls.PulsarTlsEngineProvider;
import org.apache.pulsar.common.tls.TlsPurposeContext;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.netty.ssl.DefaultSslEngineFactory;

/**
 * AsyncHttpClient {@link org.asynchttpclient.SslEngineFactory} backed by the PIP-478
 * {@link PulsarTlsEngineProvider} (the replacement for the PIP-337 {@code PulsarHttpAsyncSslEngineFactory}).
 *
 * <p>The {@link SSLContext} is resolved per the configured {@link TlsPurposeContext} from the framework's
 * cached engine provider, so certificate rotation is picked up automatically (the provider rebuilds the
 * cached context only when the underlying TLS material changes). SNI and the
 * {@code disableHttpsEndpointIdentificationAlgorithm} behaviour reproduce the previous factory exactly.
 */
public class PulsarTlsAsyncHttpSslEngineFactory extends DefaultSslEngineFactory {

    private final PulsarTlsEngineProvider engineProvider;
    private final TlsPurposeContext purpose;
    private final String host;

    /**
     * @param engineProvider the framework TLS engine provider
     * @param purpose        the client TLS purpose used to resolve material
     * @param host           the SNI host to set, or {@code null}/blank to leave SNI unset
     */
    public PulsarTlsAsyncHttpSslEngineFactory(PulsarTlsEngineProvider engineProvider, TlsPurposeContext purpose,
                                              String host) {
        this.engineProvider = engineProvider;
        this.purpose = purpose;
        this.host = host;
    }

    @Override
    protected void configureSslEngine(SSLEngine sslEngine, AsyncHttpClientConfig config) {
        super.configureSslEngine(sslEngine, config);
        if (StringUtils.isNotBlank(host)) {
            SSLParameters parameters = sslEngine.getSSLParameters();
            parameters.setServerNames(Collections.singletonList(new SNIHostName(host)));
            sslEngine.setSSLParameters(parameters);
        }
    }

    @Override
    public SSLEngine newSslEngine(AsyncHttpClientConfig config, String peerHost, int peerPort) {
        // The engine provider caches the SSLContext per purpose and rebuilds only on material change,
        // so joining here does not load material on the hot path after the first warm-up.
        SSLContext sslContext = engineProvider.getJdkSslContext(purpose).join();
        SSLEngine sslEngine = config.isDisableHttpsEndpointIdentificationAlgorithm()
                ? sslContext.createSSLEngine()
                : sslContext.createSSLEngine(domain(peerHost), peerPort);
        configureSslEngine(sslEngine, config);
        return sslEngine;
    }
}
