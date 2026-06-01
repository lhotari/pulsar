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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.pulsar.common.tls.ClientTlsPurposeContext.ClientPurpose;

/**
 * The default, file/keystore-backed {@link PulsarTlsMaterialProvider} (PIP-478).
 *
 * <p>The provider keeps a per-purpose registry of {@link TlsMaterialSource}s. A purpose is
 * normalized to a {@link RegistryKey} of {@code (purpose enum, optional usage identifier)} so that
 * the broker/proxy/web-service server purposes and the client purposes share a single, uniform
 * registry. Material resolution for {@link #getTlsMaterial(TlsPurposeContext)} proceeds in order:
 *
 * <ol>
 *   <li>the exact key including the context's {@link TlsPurposeContext#usageIdentifier()} when
 *       present;</li>
 *   <li>the purpose key without the usage identifier;</li>
 *   <li>for client purposes, the {@link ClientPurpose#fallback()} chain (server purposes have no
 *       fallback).</li>
 * </ol>
 *
 * <p>If nothing resolves and the request is the client {@link ClientPurpose#GENERIC} purpose, an
 * empty {@link DefaultClientTlsMaterial} is returned (which means "platform default trust store, no
 * client certificate"); otherwise the returned future completes exceptionally.
 *
 * <p>{@link #initialize(TlsMaterialProviderInitContext)} schedules a periodic poll on the
 * framework-supplied scheduler that reloads every registered source and notifies the registered
 * listeners for a purpose whenever its material changes (by value equality).
 *
 * <p>The registry uses copy-on-write {@link ConcurrentHashMap}s and is safe to mutate via
 * {@link #registerSource} / {@link #setSource} concurrently with in-flight
 * {@link #getTlsMaterial(TlsPurposeContext)} calls.
 */
@CustomLog
public class FileBasedTlsMaterialProvider implements PulsarTlsMaterialProvider {

    /** Default interval, in seconds, at which sources are polled for file rotation. */
    public static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 60;

    /**
     * Normalized registry key combining the well-known purpose enum and an optional usage
     * identifier. The advertised-listener name / host carried by a {@link ServerTlsPurposeContext}
     * are intentionally <em>not</em> part of the key: distinct material for distinct listeners is
     * expressed through distinct usage identifiers.
     */
    private record RegistryKey(boolean server, Enum<?> purpose, TlsPurposeContext.UsageIdentifier usageIdentifier) {

        static RegistryKey full(TlsPurposeContext ctx) {
            return new RegistryKey(ctx.isServer(), purposeEnum(ctx), ctx.usageIdentifier().orElse(null));
        }

        static RegistryKey purposeOnly(TlsPurposeContext ctx) {
            return new RegistryKey(ctx.isServer(), purposeEnum(ctx), null);
        }

        static RegistryKey clientPurpose(ClientPurpose purpose) {
            return new RegistryKey(false, purpose, null);
        }

        private static Enum<?> purposeEnum(TlsPurposeContext ctx) {
            if (ctx instanceof ClientTlsPurposeContext c) {
                return c.purpose();
            }
            return ((ServerTlsPurposeContext) ctx).purpose();
        }
    }

    /** A registered source together with the last material observed for change detection. */
    private static final class RegisteredSource {
        private final TlsMaterialSource source;
        private final AtomicReference<TlsMaterial> lastMaterial = new AtomicReference<>();

        private RegisteredSource(TlsMaterialSource source) {
            this.source = source;
        }
    }

    private final ConcurrentHashMap<RegistryKey, RegisteredSource> sources = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RegistryKey, CopyOnWriteArrayList<TlsMaterialListener>> listeners =
            new ConcurrentHashMap<>();

    private final int refreshIntervalSeconds;

    private volatile TlsMaterialProviderInitContext initContext;
    private volatile ScheduledFuture<?> pollTask;

    /**
     * Create a provider using the {@link #DEFAULT_REFRESH_INTERVAL_SECONDS default refresh interval}.
     */
    public FileBasedTlsMaterialProvider() {
        this(DEFAULT_REFRESH_INTERVAL_SECONDS);
    }

    /**
     * Create a provider with a custom file-rotation poll interval.
     *
     * @param refreshIntervalSeconds the interval, in seconds, between file-rotation polls (a value
     *                               of zero or less disables polling)
     */
    public FileBasedTlsMaterialProvider(int refreshIntervalSeconds) {
        this.refreshIntervalSeconds = refreshIntervalSeconds;
    }

    /**
     * Register a source for a purpose. Later registrations take precedence for overlapping material:
     * registering a source for a key that already has one replaces it. Material is loaded eagerly so
     * the first {@link #getTlsMaterial(TlsPurposeContext)} call does not block on disk I/O.
     *
     * @param purpose the purpose the source serves
     * @param source  the material source
     */
    public void registerSource(TlsPurposeContext purpose, TlsMaterialSource source) {
        Objects.requireNonNull(purpose, "purpose");
        Objects.requireNonNull(source, "source");
        RegistryKey key = RegistryKey.full(purpose);
        RegisteredSource registered = new RegisteredSource(source);
        sources.put(key, registered);
        warmUp(key, registered, purpose);
    }

    /**
     * Replace any source registered for a purpose outright with the supplied one. Equivalent to
     * {@link #registerSource(TlsPurposeContext, TlsMaterialSource)} for this registry implementation,
     * provided for callers that want to express "replace" intent explicitly.
     *
     * @param purpose the purpose the source serves
     * @param source  the material source
     */
    public void setSource(TlsPurposeContext purpose, TlsMaterialSource source) {
        Objects.requireNonNull(purpose, "purpose");
        Objects.requireNonNull(source, "source");
        RegistryKey key = RegistryKey.full(purpose);
        RegisteredSource registered = new RegisteredSource(source);
        sources.put(key, registered);
        warmUp(key, registered, purpose);
    }

    /**
     * Convenience registration that wraps a {@link FileBasedTlsMaterialSource} configuration in the
     * client or server source implementation matching the purpose's role.
     *
     * @param purpose the purpose the configuration serves
     * @param config  the file-based configuration
     */
    public void registerSource(TlsPurposeContext purpose, FileBasedTlsMaterialSource config) {
        Objects.requireNonNull(purpose, "purpose");
        Objects.requireNonNull(config, "config");
        TlsMaterialSource source = purpose.isServer()
                ? new FileBasedServerTlsMaterialSource(config)
                : new FileBasedClientTlsMaterialSource(config);
        registerSource(purpose, source);
    }

    private void warmUp(RegistryKey key, RegisteredSource registered, TlsPurposeContext purpose) {
        try {
            TlsMaterial material = registered.source.getTlsMaterial();
            registered.lastMaterial.set(material);
            notifyListeners(key, purpose, material);
        } catch (Exception e) {
            log.warn().attr("purpose", purpose).exception(e)
                    .log("Failed to eagerly load TLS material for source");
        }
    }

    @Override
    public CompletableFuture<Void> initialize(TlsMaterialProviderInitContext context) {
        this.initContext = Objects.requireNonNull(context, "context");
        if (refreshIntervalSeconds > 0) {
            this.pollTask = context.scheduler().scheduleWithFixedDelay(this::pollSources,
                    refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void pollSources() {
        for (var entry : sources.entrySet()) {
            RegistryKey key = entry.getKey();
            RegisteredSource registered = entry.getValue();
            try {
                TlsMaterial material = registered.source.getTlsMaterial();
                TlsMaterial previous = registered.lastMaterial.get();
                if (!Objects.equals(previous, material)) {
                    registered.lastMaterial.set(material);
                    notifyListeners(key, keyToPurpose(key), material);
                }
            } catch (Exception e) {
                log.warn().attr("purposeKey", key).exception(e)
                        .log("Failed to refresh TLS material for source");
            }
        }
    }

    @Override
    public CompletableFuture<TlsMaterial> getTlsMaterial(TlsPurposeContext purpose) {
        Objects.requireNonNull(purpose, "purpose");
        try {
            TlsMaterial material = resolve(purpose);
            if (material != null) {
                return CompletableFuture.completedFuture(material);
            }
            if (isClientGeneric(purpose)) {
                // No source configured: platform default trust store, no client certificate.
                return CompletableFuture.completedFuture(DefaultClientTlsMaterial.builder().build());
            }
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "No TLS material source registered for purpose " + purpose
                            + " and no fallback yielded material"));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private TlsMaterial resolve(TlsPurposeContext purpose) throws Exception {
        // (a) exact match including usage identifier
        if (purpose.usageIdentifier().isPresent()) {
            TlsMaterial material = currentMaterial(RegistryKey.full(purpose));
            if (material != null) {
                return material;
            }
        }
        // (b) the purpose key itself
        TlsMaterial material = currentMaterial(RegistryKey.purposeOnly(purpose));
        if (material != null) {
            return material;
        }
        // (c) the fallback chain (clients only; server purposes have no fallback)
        if (purpose instanceof ClientTlsPurposeContext client) {
            ClientPurpose fallback = client.purpose().fallback();
            while (fallback != null) {
                material = currentMaterial(RegistryKey.clientPurpose(fallback));
                if (material != null) {
                    return material;
                }
                fallback = fallback.fallback();
            }
        }
        return null;
    }

    private TlsMaterial currentMaterial(RegistryKey key) throws Exception {
        RegisteredSource registered = sources.get(key);
        if (registered == null) {
            return null;
        }
        TlsMaterial material = registered.source.getTlsMaterial();
        registered.lastMaterial.set(material);
        return material;
    }

    private static boolean isClientGeneric(TlsPurposeContext purpose) {
        return purpose instanceof ClientTlsPurposeContext client && client.purpose() == ClientPurpose.GENERIC;
    }

    @Override
    public void registerListener(TlsPurposeContext purpose, TlsMaterialListener listener) {
        Objects.requireNonNull(purpose, "purpose");
        Objects.requireNonNull(listener, "listener");
        RegistryKey key = RegistryKey.full(purpose);
        listeners.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).addIfAbsent(listener);
        // Deliver the current material immediately if it is already loaded.
        RegisteredSource registered = sources.get(key);
        if (registered == null) {
            registered = sources.get(RegistryKey.purposeOnly(purpose));
        }
        if (registered != null) {
            TlsMaterial current = registered.lastMaterial.get();
            if (current != null) {
                safeNotify(listener, purpose, current);
            }
        }
    }

    @Override
    public void unregisterListener(TlsPurposeContext purpose, TlsMaterialListener listener) {
        Objects.requireNonNull(purpose, "purpose");
        Objects.requireNonNull(listener, "listener");
        CopyOnWriteArrayList<TlsMaterialListener> set = listeners.get(RegistryKey.full(purpose));
        if (set != null) {
            set.remove(listener);
        }
    }

    private void notifyListeners(RegistryKey key, TlsPurposeContext purpose, TlsMaterial material) {
        CopyOnWriteArrayList<TlsMaterialListener> set = listeners.get(key);
        if (set == null || set.isEmpty()) {
            return;
        }
        for (TlsMaterialListener listener : set) {
            safeNotify(listener, purpose, material);
        }
    }

    private void safeNotify(TlsMaterialListener listener, TlsPurposeContext purpose, TlsMaterial material) {
        try {
            listener.onTlsMaterialUpdated(purpose, material);
        } catch (Exception e) {
            log.warn().attr("purpose", purpose).exception(e).log("TLS material listener threw");
        }
    }

    /**
     * Reconstruct a representative {@link TlsPurposeContext} from a registry key for listener
     * callbacks. The reconstructed context carries the purpose enum and usage identifier; the
     * advertised-listener name / host are not retained in the key and are therefore absent.
     */
    private static TlsPurposeContext keyToPurpose(RegistryKey key) {
        if (key.server()) {
            return new ServerTlsPurposeContext((ServerTlsPurposeContext.ServerPurpose) key.purpose(),
                    null, null, key.usageIdentifier());
        }
        return new ClientTlsPurposeContext((ClientPurpose) key.purpose(), key.usageIdentifier());
    }

    /**
     * @return the runtime context the provider was initialized with, if any
     */
    protected Optional<TlsMaterialProviderInitContext> initContext() {
        return Optional.ofNullable(initContext);
    }

    /**
     * @param purpose the purpose to inspect
     * @return an immutable snapshot of the currently registered listeners for a purpose, for testing
     */
    List<TlsMaterialListener> listenersFor(TlsPurposeContext purpose) {
        CopyOnWriteArrayList<TlsMaterialListener> set = listeners.get(RegistryKey.full(purpose));
        return set == null ? List.of() : List.copyOf(set);
    }

    @Override
    public void close() {
        ScheduledFuture<?> task = this.pollTask;
        if (task != null) {
            task.cancel(false);
            this.pollTask = null;
        }
        sources.clear();
        listeners.clear();
    }
}
