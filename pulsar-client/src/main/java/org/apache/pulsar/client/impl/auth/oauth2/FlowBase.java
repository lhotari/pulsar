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
package org.apache.pulsar.client.impl.auth.oauth2;

import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClient;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientConfig;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactory;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.Metadata;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.MetadataResolver;
import org.apache.pulsar.client.impl.auth.v5.FrameworkHttpClient;
import org.apache.pulsar.client.impl.auth.v5.FrameworkHttpClientFactory;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.PulsarHttpAsyncSslEngineFactory;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.SslEngineFactory;

/**
 * An abstract OAuth 2.0 authorization flow.
 */
@CustomLog
abstract class FlowBase implements Flow {

    public static final String CONFIG_PARAM_CONNECT_TIMEOUT = "connectTimeout";
    public static final String CONFIG_PARAM_READ_TIMEOUT = "readTimeout";
    public static final String CONFIG_PARAM_TRUST_CERTS_FILE_PATH = "trustCertsFilePath";
    public static final String CONFIG_PARAM_CERT_FILE = "tlsCertFile";
    public static final String CONFIG_PARAM_TLS_KEY_FILE = "tlsKeyFile";
    public static final String CONFIG_PARAM_AUTO_CERT_REFRESH_DURATION = "autoCertRefreshDuration";
    public static final String CONFIG_PARAM_WELL_KNOWN_METADATA_PATH = "wellKnownMetadataPath";

    protected static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    protected static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);
    protected static final Duration DEFAULT_AUTO_CERT_REFRESH_DURATION = Duration.ofSeconds(300);

    private static final long serialVersionUID = 1L;

    protected final URL issuerUrl;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final String trustCertsFilePath;
    private final String certFile;
    private final String keyFile;
    private final long autoCertRefreshSeconds;
    protected final String wellKnownMetadataPath;

    protected transient PulsarSslFactory sslFactory;
    protected transient ScheduledExecutorService sslRefreshScheduler;
    protected transient Metadata metadata;
    // PIP-478 stage 3c: the framework HTTP client factory, late-bound by AuthenticationOAuth2 from the
    // client's ClientAuthenticationServices before initialize(); null when the plugin runs outside a
    // v5-bound client (plain v4 usage), which selects the deprecated legacy private client below.
    private transient PulsarHttpClientFactory httpClientFactory;
    private transient PulsarHttpClient pulsarHttpClient;

    protected FlowBase(URL issuerUrl, Duration connectTimeout, Duration readTimeout, String trustCertsFilePath,
                       String certFile, String keyFile, Duration autoCertRefreshDuration,
                       String wellKnownMetadataPath) {
        this.issuerUrl = issuerUrl;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.trustCertsFilePath = trustCertsFilePath;
        this.certFile = certFile;
        this.keyFile = keyFile;
        this.autoCertRefreshSeconds = getParameterDurationToSeconds(CONFIG_PARAM_AUTO_CERT_REFRESH_DURATION,
                autoCertRefreshDuration, DEFAULT_AUTO_CERT_REFRESH_DURATION);
        this.wellKnownMetadataPath = wellKnownMetadataPath;
        // PIP-478 stage 3c: no longer build the HTTP client eagerly here. The flow is constructed during
        // configure() — before the client's framework services (the shared HTTP client factory) are bound —
        // so the client is created lazily on first use (initialize()), once the factory is available.
    }

    /**
     * Late-bind the framework HTTP client factory (PIP-478 stage 3c). Called by
     * {@code AuthenticationOAuth2.bindClientAuthenticationServices(...)} before {@code initialize()}, so the
     * lazily-built client is the framework-managed one that shares the owning client's resources.
     *
     * @param httpClientFactory the framework factory, or {@code null} to keep the legacy private client
     */
    void bindHttpClientFactory(PulsarHttpClientFactory httpClientFactory) {
        this.httpClientFactory = httpClientFactory;
    }

    private AsyncHttpClient defaultHttpClient(Duration readTimeout, Duration connectTimeout,
                                              String trustCertsFilePath, String certFile, String keyFile) {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setCookieStore(null);
        confBuilder.setUseProxyProperties(true);
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout(
                getParameterDurationToMillis(CONFIG_PARAM_CONNECT_TIMEOUT, connectTimeout,
                        DEFAULT_CONNECT_TIMEOUT));
        confBuilder.setReadTimeout(
                getParameterDurationToMillis(CONFIG_PARAM_READ_TIMEOUT, readTimeout, DEFAULT_READ_TIMEOUT));
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        boolean hasCertFile = StringUtils.isNotBlank(certFile);
        boolean hasKeyFile = StringUtils.isNotBlank(keyFile);
        if (hasCertFile != hasKeyFile) {
            throw new IllegalArgumentException("Invalid TLS client certificate configuration: " + CONFIG_PARAM_CERT_FILE
                    + " and " + CONFIG_PARAM_TLS_KEY_FILE + " must be provided together");
        }
        if (hasCertFile && hasKeyFile) {
            try {
                PulsarSslConfiguration sslConfiguration = PulsarSslConfiguration.builder()
                        .tlsCertificateFilePath(certFile)
                        .tlsKeyFilePath(keyFile)
                        .tlsTrustCertsFilePath(trustCertsFilePath)
                        .allowInsecureConnection(false)
                        .serverMode(false)
                        .isHttps(true)
                        .build();
                sslFactory = new org.apache.pulsar.common.util.DefaultPulsarSslFactory();
                sslFactory.initialize(sslConfiguration);
                sslFactory.createInternalSslContext();
                SslEngineFactory sslEngineFactory = new PulsarHttpAsyncSslEngineFactory(sslFactory, null);
                confBuilder.setSslEngineFactory(sslEngineFactory);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid TLS client certificate configuration", e);
            }
        } else if (StringUtils.isNotBlank(trustCertsFilePath)) {
            try {
                confBuilder.setSslContext(SslContextBuilder.forClient()
                        .trustManager(new File(trustCertsFilePath))
                        .build());
            } catch (SSLException e) {
                log.error().exception(e).log("Could not set " + CONFIG_PARAM_TRUST_CERTS_FILE_PATH);
            }
        }
        return new DefaultAsyncHttpClient(confBuilder.build());
    }

    protected synchronized PulsarHttpClient getHttpClient() {
        if (pulsarHttpClient == null) {
            if (httpClientFactory != null && (!hasOwnTlsMaterial() || frameworkClientServesIdpTls())) {
                // PIP-478 stage 3c/4a: a framework-managed HTTP client that shares the owning PulsarClient's
                // event loop / timer / DNS resolver. Used when this plugin runs inside a v5-bound client and
                // either carries no IdP TLS material, or does but the client is on the new PIP-478 TLS path,
                // where this flow's IdP material has been folded into the CLIENT_OAUTH2 policy the framework
                // client serves (see AuthenticationOAuth2.idpTlsPolicy / ClientTlsFactorySupport).
                pulsarHttpClient = httpClientFactory.newHttpClient(oauth2ClientConfig());
            } else {
                // Deprecated legacy fallback (removed in PIP-478 stage 4c): a private AsyncHttpClient with its
                // own event loop and OAuth2-configured TLS, wrapped as a PulsarHttpClient. Now reached only
                // when no TLS factory is available to carry this flow's IdP mTLS / custom trust — a plain v4
                // client (no bound services), or a v5 client on the legacy TLS path.
                AsyncHttpClient legacyClient =
                        defaultHttpClient(readTimeout, connectTimeout, trustCertsFilePath, certFile, keyFile);
                scheduleSslContextRefreshIfEnabled(autoCertRefreshSeconds);
                pulsarHttpClient = new FrameworkHttpClient(legacyClient, oauth2ClientConfig(), null, null, null);
            }
        }
        return pulsarHttpClient;
    }

    /** @return whether this flow was configured with its own OAuth2 IdP TLS material (trust / cert / key). */
    private boolean hasOwnTlsMaterial() {
        return StringUtils.isNotBlank(trustCertsFilePath) || StringUtils.isNotBlank(certFile)
                || StringUtils.isNotBlank(keyFile);
    }

    /**
     * Whether the bound framework HTTP client can serve this flow's own IdP TLS material. True only on the
     * new PIP-478 TLS path, where the client's TLS factory carries the {@link TlsPurpose#CLIENT_OAUTH2}
     * policy folded from this flow's IdP material (stage 4a). On the legacy path (no factory) the framework
     * client cannot carry custom trust, so a flow with its own IdP material keeps the deprecated private
     * client (removed in stage 4c).
     */
    private boolean frameworkClientServesIdpTls() {
        return httpClientFactory instanceof FrameworkHttpClientFactory factory && factory.hasTlsFactory();
    }

    /**
     * The IdP TLS material this flow carries (the {@code trustCertsFilePath} / {@code tlsCertFile} /
     * {@code tlsKeyFile} parameters), folded into a {@link TlsPurpose#CLIENT_OAUTH2} {@link TlsPolicy} so the
     * framework HTTP client can serve IdP mTLS / custom trust on the new PIP-478 TLS path (stage 4a, issue
     * #24944). Empty when the flow carries no IdP TLS material — the OAuth2 call then resolves to the system
     * default trust store (CLIENT_OAUTH2's empty fallback).
     *
     * @return the CLIENT_OAUTH2 policy composed from this flow's IdP material, or empty
     */
    Optional<TlsPolicy> idpTlsPolicy() {
        if (!hasOwnTlsMaterial()) {
            return Optional.empty();
        }
        TlsPolicy.Builder builder = TlsPolicy.builder().format(TlsPolicy.Format.PEM);
        if (StringUtils.isNotBlank(trustCertsFilePath)) {
            builder.trustCertsFilePath(trustCertsFilePath);
        }
        if (StringUtils.isNotBlank(certFile) && StringUtils.isNotBlank(keyFile)) {
            builder.certificateFilePath(certFile).keyFilePath(keyFile);
        }
        return Optional.of(builder.build());
    }

    private PulsarHttpClientConfig oauth2ClientConfig() {
        Duration connect = getParameterDuration(CONFIG_PARAM_CONNECT_TIMEOUT, connectTimeout, DEFAULT_CONNECT_TIMEOUT);
        Duration read = getParameterDuration(CONFIG_PARAM_READ_TIMEOUT, readTimeout, DEFAULT_READ_TIMEOUT);
        return PulsarHttpClientConfig.builder(TlsPurpose.CLIENT_OAUTH2)
                .connectTimeout(connect)
                .readTimeout(read)
                .requestTimeout(read)
                .userAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()))
                .build();
    }

    private void scheduleSslContextRefreshIfEnabled(long refreshSeconds) {
        if (sslFactory == null || refreshSeconds <= 0 || sslRefreshScheduler != null) {
            return;
        }
        sslRefreshScheduler = Executors.newSingleThreadScheduledExecutor(
                new ExecutorProvider.ExtendedThreadFactory("oauth2-tls-cert-refresher", true));
        sslRefreshScheduler.scheduleWithFixedDelay(this::refreshSslContext,
                refreshSeconds, refreshSeconds, TimeUnit.SECONDS);
        log.info().attr("refreshSeconds", refreshSeconds).log("Scheduled TLS certificate refresh");
    }

    private void refreshSslContext() {
        if (this.sslFactory == null) {
            return;
        }
        try {
            this.sslFactory.update();
            log.debug("Successfully refreshed SSL context");
        } catch (Exception e) {
            log.error().exception(e).log("Failed to refresh SSL context");
        }
    }

    private int getParameterDurationToMillis(String name, Duration value, Duration defaultValue) {
        return (int) getParameterDuration(name, value, defaultValue).toMillis();
    }

    private long getParameterDurationToSeconds(String name, Duration value, Duration defaultValue) {
        return getParameterDuration(name, value, defaultValue).getSeconds();
    }

    private Duration getParameterDuration(String name, Duration value, Duration defaultValue) {
        Duration duration;
        if (value == null) {
            log.debug().attr("name", name)
                    .attr("defaultValue", defaultValue)
                    .log("Configuration is using the default value");
            duration = defaultValue;
        } else {
            log.debug().attr("name", name).attr("value", value).log("Configuration");
            duration = value;
        }
        return duration;
    }

    public void initialize() throws PulsarClientException {
        try {
            this.metadata = createMetadataResolver().resolve();
        } catch (IOException e) {
            log.error().exception(e).log("Unable to retrieve OAuth 2.0 server metadata");
            throw new PulsarClientException.AuthenticationException("Unable to retrieve OAuth 2.0 server metadata");
        }
    }

    protected MetadataResolver createMetadataResolver() {
        return DefaultMetadataResolver.fromIssuerUrl(issuerUrl, getHttpClient(), wellKnownMetadataPath);
    }

    static String parseParameterString(Map<String, String> params, String name) {
        String s = params.get(name);
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Required configuration parameter: " + name);
        }
        return s;
    }

    static URL parseParameterUrl(Map<String, String> params, String name) {
        String s = params.get(name);
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Required configuration parameter: " + name);
        }
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed configuration parameter: " + name);
        }
    }

    static Duration parseParameterDuration(Map<String, String> params, String name) {
        String value = params.get(name);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Duration.parse(value);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Malformed configuration parameter: " + name, e);
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        if (sslRefreshScheduler != null) {
            sslRefreshScheduler.shutdownNow();
        }
        if (pulsarHttpClient != null) {
            // Idempotent: closes the legacy private AsyncHttpClient, or releases the framework client (which
            // the framework factory also closes on client shutdown).
            pulsarHttpClient.close();
        }
        if (sslFactory != null) {
            sslFactory.close();
        }
    }
}
