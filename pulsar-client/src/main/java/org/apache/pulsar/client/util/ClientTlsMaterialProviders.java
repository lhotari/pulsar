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

import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.tls.AuthenticationDataTlsMaterialSource;
import org.apache.pulsar.common.tls.ClientTlsPurposeContext;
import org.apache.pulsar.common.tls.DefaultTlsMaterialProviderInitContext;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialProvider;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialSource;
import org.apache.pulsar.common.tls.TlsPurposeContext;

/**
 * Helpers that build a client-side {@link FileBasedTlsMaterialProvider} from a
 * {@link ClientConfigurationData} (PIP-478), preserving the v4 behaviour where a TLS authentication
 * plugin (mTLS / keystore auth) can supply the client certificate material through its
 * {@link AuthenticationDataProvider} — now expressed as an
 * {@link AuthenticationDataTlsMaterialSource} registered on top of the file-based source.
 */
public final class ClientTlsMaterialProviders {

    private ClientTlsMaterialProviders() {
    }

    /**
     * Build a {@link FileBasedTlsMaterialSource} from the client's TLS configuration.
     *
     * @param conf the client configuration
     * @return the file/keystore-based TLS source
     */
    public static FileBasedTlsMaterialSource buildClientSource(ClientConfigurationData conf) {
        FileBasedTlsMaterialSource.Builder builder = FileBasedTlsMaterialSource.builder()
                .tlsProvider(conf.getSslProvider())
                .tlsCiphers(conf.getTlsCiphers())
                .tlsProtocols(conf.getTlsProtocols())
                .allowInsecureConnection(conf.isTlsAllowInsecureConnection())
                .hostnameVerificationEnabled(conf.isTlsHostnameVerificationEnable());
        if (conf.isUseKeyStoreTls()) {
            builder.keyStoreType(conf.getTlsKeyStoreType())
                    .keyStorePath(conf.getTlsKeyStorePath())
                    .keyStorePassword(conf.getTlsKeyStorePassword())
                    .trustStoreType(conf.getTlsTrustStoreType())
                    .trustStorePath(conf.getTlsTrustStorePath())
                    .trustStorePassword(conf.getTlsTrustStorePassword());
        } else {
            builder.trustCertsFilePath(conf.getTlsTrustCertsFilePath())
                    .certificateFilePath(conf.getTlsCertificateFilePath())
                    .keyFilePath(conf.getTlsKeyFilePath());
        }
        return builder.build();
    }

    /**
     * Create and initialize a {@link FileBasedTlsMaterialProvider} registering the client's TLS material
     * (and, when the configured authentication plugin supplies TLS data, an
     * {@link AuthenticationDataTlsMaterialSource} that takes precedence) for the given purpose.
     *
     * @param conf      the client configuration
     * @param purpose   the well-known client purpose to register the material under
     * @param host      the host passed to {@link Authentication#getAuthData(String)} when probing for
     *                  auth-plugin-supplied TLS material (may be {@code null})
     * @param scheduler the executor used for periodic file-rotation checks
     * @param instanceId a stable id for logging correlation
     * @return an initialized provider
     */
    public static FileBasedTlsMaterialProvider createInitializedProvider(ClientConfigurationData conf,
            ClientTlsPurposeContext.ClientPurpose purpose, String host, ScheduledExecutorService scheduler,
            String instanceId) {
        int refresh = conf.getAutoCertRefreshSeconds() > 0 ? conf.getAutoCertRefreshSeconds() : 300;
        FileBasedTlsMaterialProvider provider = new FileBasedTlsMaterialProvider(refresh);
        TlsPurposeContext purposeContext = ClientTlsPurposeContext.of(purpose);
        FileBasedTlsMaterialSource fileSource = buildClientSource(conf);
        provider.registerSource(purposeContext, fileSource);
        registerAuthDataSourceIfPresent(provider, conf, purposeContext, host, fileSource);
        ScheduledExecutorService rotationScheduler = scheduler != null ? scheduler
                : java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "pulsar-client-tls-refresh");
                    t.setDaemon(true);
                    return t;
                });
        provider.initialize(
                new DefaultTlsMaterialProviderInitContext(rotationScheduler, Clock.systemUTC(), instanceId)).join();
        return provider;
    }

    /**
     * If the configured authentication plugin exposes TLS material (e.g. {@code AuthenticationTls} /
     * {@code AuthenticationKeyStoreTls}), register an {@link AuthenticationDataTlsMaterialSource} so the
     * plugin-provided client certificate takes precedence over (augments) the file-based source.
     */
    private static void registerAuthDataSourceIfPresent(FileBasedTlsMaterialProvider provider,
            ClientConfigurationData conf, TlsPurposeContext purposeContext, String host,
            FileBasedTlsMaterialSource fileSource) {
        Authentication authentication = conf.getAuthentication();
        if (authentication == null) {
            return;
        }
        // Probe whether the plugin supplies TLS material at all. The probe must not permanently disable
        // the override: a plugin can legitimately fail to produce a (valid) credential at construction
        // time and only succeed later after a rotation (see TlsProducerConsumerTest dynamic-cert tests).
        boolean supportsTls;
        try {
            AuthenticationDataProvider probe = authentication.getAuthData(host);
            supportsTls = probe != null && probe.hasDataForTls();
        } catch (Exception probeFailure) {
            // The plugin threw while producing its (currently invalid) credential. It is a TLS plugin
            // by configuration intent, so still register the lazy override; it will start contributing
            // material once the plugin can produce a valid credential.
            supportsTls = pluginDeclaresTls(authentication);
        }
        if (!supportsTls) {
            return;
        }
        // Resolve the AuthenticationDataProvider LAZILY on each getTlsMaterial() call, so credential
        // rotation is picked up and a transient construction failure does not disable the override.
        provider.registerSource(purposeContext, AuthenticationDataTlsMaterialSource.ofSupplier(
                () -> {
                    try {
                        return authentication.getAuthData(host);
                    } catch (Exception e) {
                        return null;
                    }
                }, fileSource));
        return;
    }

    /**
     * Heuristic fallback: treat known TLS auth plugins (and any whose method name is "tls") as
     * TLS-material providers even when an initial credential probe fails.
     */
    private static boolean pluginDeclaresTls(Authentication authentication) {
        String method = authentication.getAuthMethodName();
        return "tls".equalsIgnoreCase(method);
    }
}
