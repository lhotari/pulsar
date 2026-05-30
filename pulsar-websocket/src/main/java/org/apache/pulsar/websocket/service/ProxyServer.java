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
package org.apache.pulsar.websocket.service;

import jakarta.servlet.Servlet;
import jakarta.servlet.ServletException;
import java.net.MalformedURLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.web.JettyRequestLogFactory;
import org.apache.pulsar.broker.web.JsonMapperProvider;
import org.apache.pulsar.broker.web.WebExecutorThreadPool;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.tls.DefaultTlsMaterialProviderInitContext;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialProvider;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialSource;
import org.apache.pulsar.common.tls.PulsarTlsEngineProvider;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext.ServerPurpose;
import org.apache.pulsar.jetty.tls.JettySslContextFactory;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NetworkConnectionLimit;
import org.eclipse.jetty.server.ProxyConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.QoSHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

@CustomLog
public class ProxyServer {
    private static final String MATCH_ALL = "/*";
    private final Server server;
    private final List<Handler> handlers = new ArrayList<>();
    private final WebSocketProxyConfiguration conf;
    private final WebExecutorThreadPool executorService;

    private ServerConnector connector;
    private ServerConnector connectorTls;
    private FileBasedTlsMaterialProvider tlsMaterialProvider;
    private ScheduledExecutorService scheduledExecutorService;

    public ProxyServer(WebSocketProxyConfiguration config)
            throws PulsarClientException, MalformedURLException, PulsarServerException {
        this.conf = config;
        executorService = new WebExecutorThreadPool(config.getNumHttpServerThreads(), "pulsar-websocket-web",
                config.getHttpServerThreadPoolQueueSize());
        this.server = new Server(executorService);
        if (config.getMaxHttpServerConnections() > 0) {
            server.addBean(new NetworkConnectionLimit(config.getMaxHttpServerConnections(), server));
        }

        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setUriCompliance(UriCompliance.LEGACY);
        if (config.isWebServiceTrustXForwardedFor()) {
            httpConfig.addCustomizer(new ForwardedRequestCustomizer());
        }
        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);

        List<ServerConnector> connectors = new ArrayList<>();

        if (config.getWebServicePort().isPresent()) {
            List<ConnectionFactory> connectionFactories = new ArrayList<>();
            if (config.isWebServiceHaProxyProtocolEnabled()) {
                connectionFactories.add(new ProxyConnectionFactory());
            }
            connectionFactories.add(httpConnectionFactory);
            connector = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
            connector.setPort(config.getWebServicePort().get());
            connectors.add(connector);
        }
        // TLS enabled connector
        if (config.getWebServicePortTls().isPresent()) {
            try {
                // PIP-478: resolve the web-socket proxy web-service TLS material through the framework
                // provider/engine. The provider owns periodic file-rotation polling; the Jetty factory
                // reads the (engine-cached, hot-reloaded) JDK SSLContext lazily on every request.
                this.scheduledExecutorService = Executors
                        .newSingleThreadScheduledExecutor(new ExecutorProvider
                                .ExtendedThreadFactory("proxy-websocket-ssl-refresh"));
                this.tlsMaterialProvider = buildTlsMaterialProvider(config);
                this.tlsMaterialProvider.initialize(new DefaultTlsMaterialProviderInitContext(
                        scheduledExecutorService, Clock.systemUTC(), "pulsar-websocket")).join();
                PulsarTlsEngineProvider engineProvider = new PulsarTlsEngineProvider(tlsMaterialProvider);
                ServerTlsPurposeContext webPurpose = ServerTlsPurposeContext.of(ServerPurpose.WEB_SERVICE);
                SslContextFactory.Server sslCtxFactory =
                        JettySslContextFactory.createSslContextFactory(config.getTlsProvider(),
                                () -> engineProvider.getJdkSslContext(webPurpose).join(),
                                config.isTlsRequireTrustedClientCertOnConnect(),
                                config.getWebServiceTlsCiphers(), config.getWebServiceTlsProtocols());
                List<ConnectionFactory> connectionFactories = new ArrayList<>();
                if (config.isWebServiceHaProxyProtocolEnabled()) {
                    connectionFactories.add(new ProxyConnectionFactory());
                }
                connectionFactories.add(new SslConnectionFactory(sslCtxFactory, httpConnectionFactory.getProtocol()));
                connectionFactories.add(httpConnectionFactory);
                // org.eclipse.jetty.server.AbstractConnectionFactory.getFactories contains similar logic
                // this is needed for TLS authentication
                if (httpConfig.getCustomizer(SecureRequestCustomizer.class) == null) {
                    // disable SNI host check for backwards compatibility with Jetty 9.x
                    boolean sniHostCheck = false;
                    httpConfig.addCustomizer(new SecureRequestCustomizer(sniHostCheck));
                }
                connectorTls = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
                connectorTls.setPort(config.getWebServicePortTls().get());
                connectors.add(connectorTls);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }

        // Limit number of concurrent HTTP connections to avoid getting out of
        // file descriptors
        connectors.stream().forEach(c -> c.setAcceptQueueSize(config.getHttpServerAcceptQueueSize()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));
    }

    // WebSocket endpoints use the modern Jetty 12 WebSocket API on the ee10 / jakarta.servlet stack
    // (PIP-472), registered into an ee10 Jetty context that coexists with the ee10 REST contexts.
    public void addWebSocketServlet(String basePath, Servlet socketServlet)
            throws ServletException {
        ServletHolder servletHolder = new ServletHolder("ws-events", socketServlet);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(basePath);
        context.addServlet(servletHolder, MATCH_ALL);
        org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer
                .configure(context, null);
        handlers.add(context);
    }

    public void addRestResource(String basePath, String attribute, Object attributeValue, Class<?> resourceClass) {
        ResourceConfig config = new ResourceConfig();
        config.register(resourceClass);
        config.register(JsonMapperProvider.class);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(basePath);
        context.addServlet(servletHolder, MATCH_ALL);
        context.setAttribute(attribute, attributeValue);
        handlers.add(context);
    }

    public void start() throws PulsarServerException {
        log.info()
                .attr("port", Arrays.stream(server.getConnectors())
                        .map(ServerConnector.class::cast)
                        .map(ServerConnector::getPort)
                        .map(Object::toString)
                        .collect(Collectors.joining(",")))
                .log("Starting web socket proxy at port");
        boolean showDetailedAddresses = conf.getWebServiceLogDetailedAddresses() != null
                ? conf.getWebServiceLogDetailedAddresses() :
                (conf.isWebServiceHaProxyProtocolEnabled() || conf.isWebServiceTrustXForwardedFor());
        server.setRequestLog(JettyRequestLogFactory.createRequestLogger(showDetailedAddresses, server));

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(handlers);
        Handler serverHandler = contexts;
        if (conf.getMaxConcurrentHttpRequests() > 0) {
            QoSHandler qoSHandler = new QoSHandler(serverHandler);
            qoSHandler.setMaxRequestCount(conf.getMaxConcurrentHttpRequests());
            serverHandler = qoSHandler;
        }
        server.setHandler(serverHandler);

        try {
            server.start();
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    public void stop() throws Exception {
        server.stop();
        executorService.stop();
        if (tlsMaterialProvider != null) {
            tlsMaterialProvider.close();
            tlsMaterialProvider = null;
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    public Optional<Integer> getListenPortHTTP() {
        if (connector != null) {
            return Optional.of(connector.getLocalPort());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Integer> getListenPortHTTPS() {
        if (connectorTls != null) {
            return Optional.of(connectorTls.getLocalPort());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Build an (uninitialized) {@link FileBasedTlsMaterialProvider} for the web-socket proxy's
     * web-service TLS material (PIP-478), registered for the {@link ServerPurpose#WEB_SERVICE} purpose.
     * The provider's file-rotation poll interval mirrors {@code tlsCertRefreshCheckDurationSec}.
     *
     * @param config the web-socket proxy configuration
     * @return an uninitialized provider
     */
    protected FileBasedTlsMaterialProvider buildTlsMaterialProvider(WebSocketProxyConfiguration config) {
        long refresh = config.getTlsCertRefreshCheckDurationSec();
        int refreshSeconds = refresh <= 0 ? 300 : (int) Math.min(refresh, Integer.MAX_VALUE);
        FileBasedTlsMaterialProvider provider = new FileBasedTlsMaterialProvider(refreshSeconds);
        FileBasedTlsMaterialSource.Builder builder = FileBasedTlsMaterialSource.builder()
                .tlsProvider(config.getTlsProvider())
                .tlsCiphers(config.getWebServiceTlsCiphers())
                .tlsProtocols(config.getWebServiceTlsProtocols())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(config.isTlsRequireTrustedClientCertOnConnect());
        if (config.isTlsEnabledWithKeyStore()) {
            builder.keyStoreType(config.getTlsKeyStoreType())
                    .keyStorePath(config.getTlsKeyStore())
                    .keyStorePassword(config.getTlsKeyStorePassword())
                    .trustStoreType(config.getTlsTrustStoreType())
                    .trustStorePath(config.getTlsTrustStore())
                    .trustStorePassword(config.getTlsTrustStorePassword());
        } else {
            builder.certificateFilePath(config.getTlsCertificateFilePath())
                    .keyFilePath(config.getTlsKeyFilePath())
                    .trustCertsFilePath(config.getTlsTrustCertsFilePath());
        }
        provider.registerSource(ServerTlsPurposeContext.of(ServerPurpose.WEB_SERVICE), builder.build());
        return provider;
    }
}
