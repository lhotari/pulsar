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
package org.apache.pulsar.client.impl.v5.http;

import io.netty.handler.codec.http.HttpHeaderNames;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.http.HttpRequest;
import org.apache.pulsar.client.api.v5.http.HttpResponse;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClient;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AsyncHttpClient}-backed {@link PulsarHttpClient} (PIP-478).
 *
 * <p>Each instance owns one {@link AsyncHttpClient}; the event loop and timer are owned by the
 * {@link AsyncHttpClientFactory} that created it, so {@link #close()} only closes this client's own
 * AsyncHttpClient and leaves the shared resources to the factory/framework.
 *
 * <p>Responses are fully buffered. If a response body exceeds
 * {@link PulsarHttpClientConfig#maxResponseBodyBytes()} the returned future completes exceptionally
 * with an {@link IOException} rather than silently truncating, so callers never act on a partial body.
 */
public class AsyncHttpPulsarClient implements PulsarHttpClient {

    private static final Logger log = LoggerFactory.getLogger(AsyncHttpPulsarClient.class);

    private final AsyncHttpClient asyncHttpClient;
    private final PulsarHttpClientConfig config;

    AsyncHttpPulsarClient(AsyncHttpClient asyncHttpClient, PulsarHttpClientConfig config) {
        this.asyncHttpClient = asyncHttpClient;
        this.config = config;
    }

    @Override
    public CompletableFuture<HttpResponse> execute(HttpRequest request) {
        // Per CODING.md, a CompletableFuture-returning method must not throw synchronously: convert any
        // request-building error into an exceptionally-completed future.
        final org.asynchttpclient.Request ahcRequest;
        try {
            ahcRequest = toAhcRequest(request);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
        return asyncHttpClient.executeRequest(ahcRequest)
                .toCompletableFuture()
                .thenCompose(this::toHttpResponse);
    }

    private org.asynchttpclient.Request toAhcRequest(HttpRequest request) {
        RequestBuilder builder = new RequestBuilder(request.method().name())
                .setUrl(request.uri().toString());

        // Default headers first, then per-request headers so the latter win.
        config.defaultHeaders().forEach(builder::setHeader);
        request.headers().forEach(builder::setHeader);

        request.body().ifPresent(body -> {
            if (body instanceof HttpRequest.Bytes bytes) {
                builder.setBody(bytes.content());
                if (bytes.contentType() != null) {
                    builder.setHeader(HttpHeaderNames.CONTENT_TYPE, bytes.contentType());
                }
            } else if (body instanceof HttpRequest.Form form) {
                form.fields().forEach(builder::addFormParam);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported request body type: " + body.getClass().getName());
            }
        });

        request.timeout().ifPresent(timeout -> builder.setRequestTimeout((int) timeout.toMillis()));
        return builder.build();
    }

    private CompletableFuture<HttpResponse> toHttpResponse(Response response) {
        byte[] body = response.getResponseBodyAsBytes();
        if (body != null && body.length > config.maxResponseBodyBytes()) {
            return CompletableFuture.failedFuture(new IOException(
                    "HTTP response body of " + body.length + " bytes exceeds the configured maximum of "
                            + config.maxResponseBodyBytes() + " bytes"));
        }
        Map<String, String> headers = new LinkedHashMap<>();
        response.getHeaders().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
        return CompletableFuture.completedFuture(
                HttpResponse.of(response.getStatusCode(), headers, body));
    }

    @Override
    public void close() {
        try {
            asyncHttpClient.close();
        } catch (IOException e) {
            log.warn("Failed to close AsyncHttpClient", e);
        }
    }
}
