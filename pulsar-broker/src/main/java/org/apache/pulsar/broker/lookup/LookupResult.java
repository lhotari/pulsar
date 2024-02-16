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
package org.apache.pulsar.broker.lookup;

import static org.apache.pulsar.broker.lookup.v2.TopicLookup.LISTENERNAME_PARAM;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import javax.ws.rs.core.UriBuilder;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * Represent a lookup result.
 *
 * Result can either be the lookup data describing the broker that owns the broker or the HTTP endpoint to where we need
 * to redirect the client to try again.
 */
public class LookupResult {
    public enum Type {
        BrokerUrl, RedirectUrl, LoadManagerMigrationUrl
    }

    private final Type type;
    private final boolean authoritativeRedirect;
    @Getter
    private final String brokerServiceListenerName;
    @Getter
    private final String webServiceListenerName;
    private final LookupData lookupData;

    @Builder
    private LookupResult(String brokerId, String httpUrl, String httpUrlTls, String brokerServiceUrl,
                         String brokerServiceUrlTls, Type type, boolean authoritativeRedirect,
                         String brokerServiceListenerName, String webServiceListenerName) {
        this.type = Objects.requireNonNull(type);
        this.authoritativeRedirect = authoritativeRedirect;
        this.brokerServiceListenerName = brokerServiceListenerName;
        this.webServiceListenerName = webServiceListenerName;
        this.lookupData = new LookupData(brokerId, brokerServiceUrl, brokerServiceUrlTls, httpUrl, httpUrlTls);
    }

    public static LookupResult create(BrokerLookupData selectedBroker, LookupOptions options,
                                      boolean authoritativeRedirect) {
        return internalCreate(selectedBroker, options, Type.RedirectUrl, authoritativeRedirect);
    }

    public static LookupResult createLoadManagerMigrationLookupResult(BrokerLookupData selectedBroker,
                                                                      LookupOptions options) {
        return internalCreate(selectedBroker, options, Type.LoadManagerMigrationUrl, false);
    }

    public static LookupResult create(BrokerLookupData selectedBroker, LookupOptions options) {
        return internalCreate(selectedBroker, options, Type.BrokerUrl, false);
    }

    private static LookupResult internalCreate(BrokerLookupData selectedBroker, LookupOptions options,
                                               Type type, boolean authoritativeRedirect) {
        return internalCreate(options, type, authoritativeRedirect, selectedBroker.getBrokerId(),
                selectedBroker.getWebServiceUrl(), selectedBroker.getWebServiceUrlTls(),
                selectedBroker.getPulsarServiceUrl(), selectedBroker.getPulsarServiceUrlTls(),
                selectedBroker.advertisedListeners());
    }

    public static LookupResult create(LocalBrokerData selectedBroker, LookupOptions options,
                                      boolean authoritativeRedirect) {
        return internalCreate(selectedBroker, options, Type.RedirectUrl, authoritativeRedirect);
    }

    public static LookupResult create(LocalBrokerData selectedBroker, LookupOptions options) {
        return internalCreate(selectedBroker, options, Type.BrokerUrl, false);
    }

    private static LookupResult internalCreate(LocalBrokerData selectedBroker, LookupOptions options,
                                               Type type, boolean authoritativeRedirect) {
        return internalCreate(options, type, authoritativeRedirect, selectedBroker.getBrokerId(),
                selectedBroker.getWebServiceUrl(), selectedBroker.getWebServiceUrlTls(),
                selectedBroker.getPulsarServiceUrl(), selectedBroker.getPulsarServiceUrlTls(),
                selectedBroker.getAdvertisedListeners());
    }

    public static LookupResult create(NamespaceEphemeralData nsData, LookupOptions options) {
        return internalCreate(nsData, options, Type.BrokerUrl, false);
    }

    public static LookupResult create(NamespaceEphemeralData nsData, LookupOptions options,
                                      boolean authoritativeRedirect) {
        return internalCreate(nsData, options, Type.RedirectUrl, authoritativeRedirect);
    }

    private static LookupResult internalCreate(NamespaceEphemeralData nsData, LookupOptions options, Type type,
                                               boolean authoritativeRedirect) {
        return internalCreate(options, type, authoritativeRedirect, nsData.getBrokerId(), nsData.getHttpUrl(),
                nsData.getHttpUrlTls(), nsData.getNativeUrl(), nsData.getNativeUrlTls(),
                nsData.getAdvertisedListeners());
    }

    private static LookupResult internalCreate(LookupOptions options, Type type,
                                               boolean authoritativeRedirect, String brokerId, String webServiceUrl,
                                               String webServiceUrlTls, String pulsarServiceUrl,
                                               String pulsarServiceUrlTls,
                                               Map<String, AdvertisedListener> advertisedListeners) {
        String httpUrl = webServiceUrl;
        String httpUrlTls = webServiceUrlTls;
        String brokerServiceUrl = pulsarServiceUrl;
        String brokerServiceUrlTls = pulsarServiceUrlTls;
        String brokerServiceListenerName = null;
        String webServiceListenerName = null;

        if (options != null && options.hasAdvertisedListenerName()) {
            String advertisedListenerName = options.getAdvertisedListenerName();
            var advertisedListener = advertisedListeners.get(advertisedListenerName);
            if (advertisedListener != null) {
                brokerServiceListenerName = advertisedListenerName;
                brokerServiceUrl = null;
                brokerServiceUrlTls = null;
                if (advertisedListener.getBrokerServiceUrl() != null) {
                    brokerServiceUrl = advertisedListener.getBrokerServiceUrl().toString();
                }
                if (advertisedListener.getBrokerServiceUrlTls() != null) {
                    brokerServiceUrlTls = advertisedListener.getBrokerServiceUrlTls().toString();
                }
            }
        }

        if (options != null && options.hasWebServiceAdvertisedListenerName()) {
            String advertisedListenerName = options.getWebServiceAdvertisedListenerName();
            var advertisedListener = advertisedListeners.get(advertisedListenerName);
            if (advertisedListener != null) {
                webServiceListenerName = advertisedListenerName;
                httpUrl = null;
                httpUrlTls = null;
                if (advertisedListener.getBrokerHttpUrl() != null) {
                    httpUrl = advertisedListener.getBrokerHttpUrl().toString();
                }
                if (advertisedListener.getBrokerHttpsUrl() != null) {
                    httpUrlTls = advertisedListener.getBrokerHttpsUrl().toString();
                }
                // default to use the webServiceAdvertisedListenerName as the brokerServiceListenerName if there
                // is a brokerServiceUrl or brokerServiceUrlTls configured for the webServiceAdvertisedListenerName
                if (brokerServiceListenerName == null && (advertisedListener.getBrokerServiceUrl() != null
                        || advertisedListener.getBrokerServiceUrlTls() != null)) {
                    brokerServiceListenerName = advertisedListenerName;
                    brokerServiceUrl = null;
                    brokerServiceUrlTls = null;
                    if (advertisedListener.getBrokerServiceUrl() != null) {
                        brokerServiceUrl = advertisedListener.getBrokerServiceUrl().toString();
                    }
                    if (advertisedListener.getBrokerServiceUrlTls() != null) {
                        brokerServiceUrlTls = advertisedListener.getBrokerServiceUrlTls().toString();
                    }
                }
            }
        }

        // for backwards compatibility, parse the brokerId from webServiceUrl or webServiceUrlTls
        // this might be the case temporarily when the broker upgrade happens and there are mixed versions
        // of brokers in the cluster
        if (brokerId == null && (webServiceUrl != null || webServiceUrlTls != null)) {
            if (webServiceUrl != null) {
                brokerId = webServiceUrl.substring("http://".length());
            } else {
                brokerId = webServiceUrlTls.substring("https://".length());
            }
        }

        return builder()
                .type(type)
                .brokerId(brokerId)
                .httpUrl(httpUrl)
                .httpUrlTls(httpUrlTls)
                .brokerServiceUrl(brokerServiceUrl)
                .brokerServiceUrlTls(brokerServiceUrlTls)
                .authoritativeRedirect(authoritativeRedirect)
                .brokerServiceListenerName(brokerServiceListenerName)
                .webServiceListenerName(webServiceListenerName)
                .build();
    }

    public boolean isBrokerUrl() {
        return type == Type.BrokerUrl;
    }

    public boolean isRedirect() {
        return type == Type.RedirectUrl || type == Type.LoadManagerMigrationUrl;
    }

    public boolean isLoadManagerMigration() {
        return type == Type.LoadManagerMigrationUrl;
    }

    public boolean isAuthoritativeRedirect() {
        return authoritativeRedirect;
    }

    public LookupData getLookupData() {
        return lookupData;
    }

    /**
     * Creates a redirect URI by replacing the host, port and scheme of the given request URI with the
     * web service URL of this lookup result. The original path and query parameters are preserved.
     * The {@code authoritative} query parameter is set from this lookup result's
     * {@link #isAuthoritativeRedirect()} value when the result is a redirect, and removed otherwise.
     *
     * @param requestUri the incoming request URI (its scheme decides whether the HTTP or HTTPS
     *                   broker URL is used as the redirect target)
     * @return the redirect URI
     */
    public URI toRedirectUri(URI requestUri) {
        return toRedirectUriInternal(requestUri, this.authoritativeRedirect);
    }

    /**
     * Same as {@link #toRedirectUri(URI)} but overrides the {@code authoritative} query parameter
     * with the supplied value (e.g. when the current broker is the leader and must mark the
     * redirect as authoritative regardless of what the lookup result carried).
     */
    public URI toRedirectUri(URI requestUri, boolean authoritativeRedirectOverride) {
        return toRedirectUriInternal(requestUri, authoritativeRedirectOverride);
    }

    private URI toRedirectUriInternal(URI requestUri, boolean authoritativeRedirect) {
        boolean requireHttps = "https".equalsIgnoreCase(requestUri.getScheme());
        String webServiceUrl = requireHttps ? lookupData.getHttpUrlTls() : lookupData.getHttpUrl();
        Objects.requireNonNull(webServiceUrl, () -> "No "
                + (requireHttps ? "https" : "http")
                + " URL configured for broker " + lookupData.getBrokerId());
        URI webServiceUri = URI.create(webServiceUrl);
        UriBuilder uriBuilder =
                UriBuilder.fromUri(requestUri) // use the path and query parameters from the request URI
                        .scheme(webServiceUri.getScheme()) // use the schema from the lookup result
                        .host(webServiceUri.getHost())  // use the host from the lookup result
                        .port(webServiceUri.getPort()); // use the port from the lookup result
        if (isRedirect()) {
            // pass the authoritative parameter only when the type is redirect
            uriBuilder.replaceQueryParam("authoritative", authoritativeRedirect);
        } else {
            // remove the parameter when the type is not redirect
            uriBuilder.replaceQueryParam("authoritative");
        }
        // pass the listener parameter
        if (StringUtils.isNotBlank(brokerServiceListenerName)) {
            uriBuilder.replaceQueryParam(LISTENERNAME_PARAM, brokerServiceListenerName);
        } else {
            // remove the parameter when the listener name is not specified
            uriBuilder.replaceQueryParam(LISTENERNAME_PARAM);
        }
        return uriBuilder.build();
    }

    @Override
    public String toString() {
        return "LookupResult{"
                + "type=" + type
                + ", lookupData=" + lookupData
                + ", authoritativeRedirect=" + authoritativeRedirect
                + ", brokerServiceListenerName='" + brokerServiceListenerName + '\''
                + ", webServiceListenerName='" + webServiceListenerName + '\''
                + '}';
    }
}
