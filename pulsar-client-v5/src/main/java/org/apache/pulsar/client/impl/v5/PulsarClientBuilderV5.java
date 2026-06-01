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
package org.apache.pulsar.client.impl.v5;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.config.TlsPolicy;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.v5.auth.V5ToV4AuthenticationAdapter;

/**
 * V5 implementation of PulsarClientBuilder.
 * Builds a v4 ClientConfigurationData internally and wraps the v4 PulsarClientImpl.
 */
final class PulsarClientBuilderV5 implements PulsarClientBuilder {

    private final ClientConfigurationData conf = new ClientConfigurationData();
    private String description;
    private Duration transactionTimeout;
    private ScheduledExecutorService authScheduler;

    PulsarClientBuilderV5() {
        conf.setStatsIntervalSeconds(0);
    }

    @Override
    public PulsarClient build() throws PulsarClientException {
        try {
            var v4Client = new PulsarClientImpl(conf);
            return new PulsarClientV5(v4Client, description, transactionTimeout);
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        }
    }

    @Override
    public PulsarClientBuilder serviceUrl(String serviceUrl) {
        validatePulsarServiceUrl(serviceUrl, "serviceUrl");
        conf.setServiceUrl(serviceUrl);
        return this;
    }

    @Override
    public PulsarClientBuilder authentication(Authentication authentication) {
        // Bridge the v5 auth plugin to the v4 Authentication interface that the underlying
        // PulsarClientImpl / ClientCnx drive. The adapter also implements AsyncAuthenticationDriver,
        // so ClientCnx routes auth through the non-blocking async path. The adapter needs an executor
        // to off-load any (legacy) blocking auth work and a stable client-instance id for logging.
        // TODO PIP-478 follow-up: reuse the v4 client's internal scheduler / instance id / HTTP client
        // factory once they are exposed before PulsarClientImpl construction; for now we create a
        // dedicated daemon scheduler so the module is self-contained and compiles.
        ScheduledExecutorService scheduler = authScheduler();
        String clientInstanceId = "pulsar-client-v5-" + UUID.randomUUID();
        V5ToV4AuthenticationAdapter adapter = new V5ToV4AuthenticationAdapter(
                authentication, scheduler, null, clientInstanceId, null);
        conf.setAuthentication(adapter);
        return this;
    }

    /**
     * Lazily create the shared daemon scheduler used to off-load authentication work for the
     * {@link V5ToV4AuthenticationAdapter}. A single thread is sufficient: auth work is infrequent and
     * already asynchronous.
     *
     * @return the auth scheduler
     */
    private ScheduledExecutorService authScheduler() {
        if (authScheduler == null) {
            authScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pulsar-client-v5-auth");
                t.setDaemon(true);
                return t;
            });
        }
        return authScheduler;
    }

    @Override
    public PulsarClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws PulsarClientException {
        conf.setAuthPluginClassName(authPluginClassName);
        conf.setAuthParams(authParamsString);
        return this;
    }

    @Override
    public PulsarClientBuilder operationTimeout(Duration timeout) {
        conf.setOperationTimeoutMs(timeout.toMillis());
        return this;
    }

    @Override
    public PulsarClientBuilder connectionPolicy(ConnectionPolicy policy) {
        conf.setConnectionTimeoutMs((int) policy.connectionTimeout().toMillis());
        conf.setConnectionsPerBroker(policy.connectionsPerBroker());
        conf.setUseTcpNoDelay(policy.enableTcpNoDelay());
        conf.setKeepAliveIntervalSeconds((int) policy.keepAliveInterval().toSeconds());
        conf.setConnectionMaxIdleSeconds((int) policy.connectionMaxIdleTime().toSeconds());
        conf.setNumIoThreads(policy.ioThreads());
        conf.setNumListenerThreads(policy.callbackThreads());
        if (policy.proxyServiceUrl() != null) {
            validatePulsarServiceUrl(policy.proxyServiceUrl(), "ConnectionPolicy.proxyServiceUrl");
            conf.setProxyServiceUrl(policy.proxyServiceUrl());
            if (policy.proxyProtocol() != null) {
                conf.setProxyProtocol(
                        org.apache.pulsar.client.api.ProxyProtocol.valueOf(policy.proxyProtocol().name()));
            }
        }
        // BackoffPolicy adaptation will be implemented when the v4 client exposes
        // a public way to override the reconnection backoff.
        return this;
    }

    @Override
    public PulsarClientBuilder transactionPolicy(TransactionPolicy policy) {
        conf.setEnableTransaction(true);
        this.transactionTimeout = policy.timeout();
        return this;
    }

    @Override
    public PulsarClientBuilder tlsPolicy(TlsPolicy policy) {
        // PIP-478: the v5 TlsPolicy describes the client's TLS material; we map it onto the underlying
        // configuration, from which the framework's PulsarTlsMaterialProvider (FileBasedTlsMaterialProvider)
        // loads, caches and rotates the material. mTLS is configured here at the client level (key + cert),
        // not in an authentication plugin.
        conf.setUseTls(true);
        if (policy.trustCertsFilePath() != null) {
            conf.setTlsTrustCertsFilePath(policy.trustCertsFilePath());
        }
        if (policy.certificateFilePath() != null) {
            conf.setTlsCertificateFilePath(policy.certificateFilePath());
        }
        if (policy.keyFilePath() != null) {
            conf.setTlsKeyFilePath(policy.keyFilePath());
        }
        conf.setTlsAllowInsecureConnection(policy.allowInsecureConnection());
        conf.setTlsHostnameVerificationEnable(policy.enableHostnameVerification());
        return this;
    }

    @Override
    public PulsarClientBuilder openTelemetry(OpenTelemetry openTelemetry) {
        conf.setOpenTelemetry(openTelemetry);
        return this;
    }

    @Override
    public PulsarClientBuilder memoryLimit(MemorySize size) {
        conf.setMemoryLimitBytes(size.bytes());
        return this;
    }

    @Override
    public PulsarClientBuilder listenerName(String name) {
        conf.setListenerName(name);
        return this;
    }

    @Override
    public PulsarClientBuilder description(String description) {
        this.description = description;
        conf.setDescription(description);
        return this;
    }

    /**
     * Reject anything that isn't the broker binary protocol. The most common
     * mistake is passing the admin/web service URL ({@code http://...}) where a
     * broker URL is expected — call that out specifically. The v4 client used to
     * silently fail far downstream with cryptic connection errors; here we fail
     * fast at configure time with a message the user can act on.
     */
    private static void validatePulsarServiceUrl(String url, String fieldName) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be null or blank");
        }
        if (url.startsWith("pulsar://") || url.startsWith("pulsar+ssl://")) {
            return;
        }
        if (url.startsWith("http://") || url.startsWith("https://")) {
            throw new IllegalArgumentException(fieldName + " must use the broker binary protocol "
                    + "(pulsar:// or pulsar+ssl://); got '" + url + "'. This looks like the admin/web "
                    + "service URL — pass the broker service URL instead (typically port 6650, or "
                    + "6651 for TLS).");
        }
        throw new IllegalArgumentException(fieldName + " must use the broker binary protocol "
                + "(pulsar:// or pulsar+ssl://); got '" + url + "'.");
    }
}
