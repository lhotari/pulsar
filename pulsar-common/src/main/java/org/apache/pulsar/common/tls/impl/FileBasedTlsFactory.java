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

import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import lombok.CustomLog;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsEndpoint;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;

/**
 * The default, file-based {@link PulsarTlsFactory} (PIP-478).
 *
 * <p>Immutable after construction: the complete {@code TlsPurpose -> TlsPolicy} map and the
 * factory-wide {@link FileBasedTlsFactorySettings} are fixed by the constructor and never mutated. The
 * owning component (the v5 client builder, or {@code DefaultBrokerTlsFactory} on the server side)
 * composes the final map before constructing the factory.
 *
 * <p>Natively supplies the Netty {@code SslContext} (on the configured engine — JDK or OpenSSL) and the
 * JDK {@code SSLContext}; returns {@code empty()} for every other class (notably Jetty's
 * {@code SslContextFactory.Server}), which the framework synthesizes from the JDK {@code SSLContext}.
 *
 * <p><b>Purpose resolution.</b> A request resolves the requested purpose, then walks its single-level
 * {@link TlsPurpose#fallback() fallback} chain; the first purpose configured with a policy wins. When
 * nothing along the chain is configured, a {@code CLIENT}-role purpose resolves to the system default
 * (OS trust store, no client certificate) and a {@code SERVER}-role purpose fails the request. A
 * resolved-but-unbuildable request completes the future <em>exceptionally</em> — never {@code empty()},
 * which strictly means "unsupported {@code (purpose, class)} combination".
 *
 * <p><b>Rotation.</b> One {@link TlsMaterialSource} is shared per configured purpose. Server-side
 * subscribers are pushed rebuilt instances by a background poll (interval from the settings, on the
 * framework scheduler); client-side one-shot callers re-stat on each request and pick up rotation
 * naturally. A failed rebuild keeps the last-good instance, logs at WARN, and retries on the next
 * change; a subscriber callback that throws is caught and logged, and the subscription stays live.
 */
@CustomLog
public class FileBasedTlsFactory implements PulsarTlsFactory {

    private final Map<TlsPurpose, RegisteredSource> registry;
    private final FileBasedTlsFactorySettings settings;

    private volatile TlsFactoryInitContext initContext;
    private volatile RegisteredSource systemDefaultSource;
    private volatile ScheduledFuture<?> pollFuture;
    private volatile boolean closed;

    /**
     * Construct an immutable file-based factory.
     *
     * @param policies the complete purpose&rarr;policy map (defensively copied)
     * @param settings the factory-wide engine/refresh settings
     */
    public FileBasedTlsFactory(Map<TlsPurpose, TlsPolicy> policies, FileBasedTlsFactorySettings settings) {
        Objects.requireNonNull(policies, "policies must not be null");
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        Map<TlsPurpose, RegisteredSource> built = new LinkedHashMap<>();
        for (Map.Entry<TlsPurpose, TlsPolicy> entry : policies.entrySet()) {
            TlsPurpose purpose = Objects.requireNonNull(entry.getKey(), "purpose key must not be null");
            TlsPolicy policy = Objects.requireNonNull(entry.getValue(), "policy must not be null");
            built.put(purpose, new RegisteredSource(purpose, policy, new TlsMaterialSource(policy)));
        }
        this.registry = Map.copyOf(built);
    }

    @Override
    public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
        try {
            this.initContext = Objects.requireNonNull(context, "context must not be null");
            int interval = settings.refreshIntervalSeconds();
            if (interval > 0 && context.scheduler() != null) {
                this.pollFuture = context.scheduler().scheduleWithFixedDelay(
                        this::pollSafely, interval, interval, TimeUnit.SECONDS);
            }
            log.debug().attr("purposes", registry.keySet()).attr("refreshIntervalSeconds", interval)
                    .log("Initialized FileBasedTlsFactory");
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose, Class<T> instanceClass) {
        return createOneShot(purpose, instanceClass);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, TlsEndpoint endpoint, Class<T> instanceClass) {
        // The default file-based factory serves purpose-scoped material and ignores the endpoint hint.
        return createOneShot(purpose, instanceClass);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
        Objects.requireNonNull(onLoadOrReload, "onLoadOrReload must not be null");
        if (!isSupported(instanceClass)) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return runAsync(() -> {
            RegisteredSource source = resolve(purpose);
            synchronized (source) {
                TlsMaterial material = source.currentMaterial();
                T instance = source.acquireInstance(instanceClass, material, settings);
                Subscription<T> subscription = new Subscription<>(instanceClass, onLoadOrReload);
                // Initial delivery happens-before the returned future completes (same thread, ordered).
                subscription.deliver(instance);
                source.subscriptions.add(subscription);
                return Optional.of((TlsHandle<T>) new SubscriptionHandle<>(source, subscription));
            }
        });
    }

    private <T> CompletableFuture<Optional<TlsHandle<T>>> createOneShot(TlsPurpose purpose, Class<T> instanceClass) {
        if (!isSupported(instanceClass)) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return runAsync(() -> {
            RegisteredSource source = resolve(purpose);
            synchronized (source) {
                TlsMaterial material = source.currentMaterial();
                T instance = source.acquireInstance(instanceClass, material, settings);
                retain(instance);
                return Optional.of((TlsHandle<T>) new OneShotHandle<>(instance));
            }
        });
    }

    @Override
    public void close() {
        closed = true;
        ScheduledFuture<?> poll = this.pollFuture;
        if (poll != null) {
            poll.cancel(false);
        }
        for (RegisteredSource source : registry.values()) {
            source.releaseAll();
        }
        RegisteredSource systemDefault = this.systemDefaultSource;
        if (systemDefault != null) {
            systemDefault.releaseAll();
        }
    }

    /**
     * Resolve a requested purpose to the {@link RegisteredSource} that owns its material, following the
     * single-level fallback chain and the role's empty-fallback rule.
     *
     * @throws TlsMaterialUnavailableException when a server-role purpose has no material configured
     */
    private RegisteredSource resolve(TlsPurpose purpose) {
        Objects.requireNonNull(purpose, "purpose must not be null");
        for (TlsPurpose candidate = purpose; candidate != null; candidate = candidate.fallback().orElse(null)) {
            RegisteredSource source = registry.get(candidate);
            if (source != null) {
                return source;
            }
        }
        if (purpose.role() == TlsPurpose.Role.CLIENT) {
            return systemDefaultSource();
        }
        throw new TlsMaterialUnavailableException(
                "No TLS material configured for server purpose " + purpose + " (and it has no fallback)");
    }

    private RegisteredSource systemDefaultSource() {
        RegisteredSource existing = this.systemDefaultSource;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (this.systemDefaultSource == null) {
                // System default: verify hostnames, OS trust store, no client certificate. A constant
                // source (no files, never rotates).
                TlsPolicy defaultPolicy = TlsPolicy.builder().build();
                this.systemDefaultSource = new RegisteredSource(
                        TlsPurpose.CLIENT_DEFAULT, defaultPolicy, null);
            }
            return this.systemDefaultSource;
        }
    }

    private void pollSafely() {
        if (closed) {
            return;
        }
        try {
            for (RegisteredSource source : registry.values()) {
                source.poll(settings);
            }
        } catch (Throwable t) {
            log.warn().exception(t).log("Unexpected error during TLS material poll");
        }
    }

    private <R> CompletableFuture<R> runAsync(Callable<R> task) {
        CompletableFuture<R> future = new CompletableFuture<>();
        TlsFactoryInitContext context = this.initContext;
        Executor executor = context != null && context.blockingExecutor() != null
                ? context.blockingExecutor() : Runnable::run;
        try {
            executor.execute(() -> {
                try {
                    future.complete(task.call());
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    private static boolean isSupported(Class<?> instanceClass) {
        return instanceClass == SslContext.class || instanceClass == SSLContext.class;
    }

    private static void retain(Object instance) {
        if (instance != null) {
            ReferenceCountUtil.retain(instance);
        }
    }

    private static void release(Object instance) {
        if (instance != null) {
            ReferenceCountUtil.release(instance);
        }
    }

    /**
     * A configured purpose's material source together with the cached, factory-owned context instances
     * and the active subscriptions. All state is guarded by the instance monitor.
     */
    private static final class RegisteredSource {
        private final TlsPurpose purpose;
        private final TlsPolicy policy;
        // Null for the system-default source, whose constant material never rotates.
        private final TlsMaterialSource source;
        private final CopyOnWriteArrayList<Subscription<?>> subscriptions = new CopyOnWriteArrayList<>();

        private SslContext nettyContext;
        private TlsMaterial nettyMaterial;
        private SSLContext jdkContext;
        private TlsMaterial jdkMaterial;

        RegisteredSource(TlsPurpose purpose, TlsPolicy policy, TlsMaterialSource source) {
            this.purpose = purpose;
            this.policy = policy;
            this.source = source;
        }

        synchronized TlsMaterial currentMaterial() throws Exception {
            if (source == null) {
                return TlsMaterial.SYSTEM_DEFAULT;
            }
            return source.refresh().material();
        }

        /**
         * Return the cached context of the requested class, rebuilding it (and releasing the superseded
         * one) only when the material changed in value. The returned instance is the factory-owned memo;
         * callers that hand it to a consumer {@link FileBasedTlsFactory#retain(Object) retain} it.
         */
        synchronized <T> T acquireInstance(Class<T> instanceClass, TlsMaterial material,
                                           FileBasedTlsFactorySettings settings) throws Exception {
            if (instanceClass == SslContext.class) {
                if (nettyContext == null || !material.equals(nettyMaterial)) {
                    SslContext built = purpose.role() == TlsPurpose.Role.SERVER
                            ? TlsContexts.buildNettyServerContext(material, policy, settings.engineProvider(),
                                    settings.requireTrustedClientCert())
                            : TlsContexts.buildNettyClientContext(material, policy, settings.engineProvider());
                    release(nettyContext);
                    nettyContext = built;
                    nettyMaterial = material;
                }
                return instanceClass.cast(nettyContext);
            }
            if (instanceClass == SSLContext.class) {
                if (jdkContext == null || !material.equals(jdkMaterial)) {
                    jdkContext = TlsContexts.buildJdkContext(material, policy);
                    jdkMaterial = material;
                }
                return instanceClass.cast(jdkContext);
            }
            throw new IllegalArgumentException("Unsupported instance class " + instanceClass);
        }

        synchronized void poll(FileBasedTlsFactorySettings settings) {
            if (source == null || subscriptions.isEmpty()) {
                return;
            }
            TlsMaterialSource.RefreshOutcome outcome;
            try {
                outcome = source.refresh();
            } catch (Exception e) {
                // Keep-last-good: leave every subscription on its current instance and retry next change.
                log.warn().attr("purpose", purpose).exception(e)
                        .log("Failed to reload TLS material; keeping the last-good instance");
                return;
            }
            if (!outcome.changed()) {
                return;
            }
            for (Subscription<?> subscription : subscriptions) {
                try {
                    Object rebuilt = acquireInstance(subscription.instanceClass, outcome.material(), settings);
                    subscription.deliverErased(rebuilt);
                } catch (Exception e) {
                    log.warn().attr("purpose", purpose).attr("class", subscription.instanceClass.getName())
                            .exception(e).log("Failed to rebuild rotated TLS instance; keeping the last-good one");
                }
            }
        }

        synchronized void removeSubscription(Subscription<?> subscription) {
            if (subscriptions.remove(subscription)) {
                subscription.releaseCurrent();
            }
        }

        synchronized void releaseAll() {
            for (Subscription<?> subscription : subscriptions) {
                subscription.releaseCurrent();
            }
            subscriptions.clear();
            release(nettyContext);
            nettyContext = null;
            nettyMaterial = null;
            jdkContext = null;
            jdkMaterial = null;
        }
    }

    /** A live server-side subscription: its instance class, callback, and last-delivered instance. */
    private static final class Subscription<T> {
        private final Class<T> instanceClass;
        private final Consumer<T> callback;
        private T current;

        Subscription(Class<T> instanceClass, Consumer<T> callback) {
            this.instanceClass = instanceClass;
            this.callback = callback;
        }

        void deliver(T instance) {
            retain(instance);
            release(current);
            current = instance;
            safeInvoke(instance);
        }

        @SuppressWarnings("unchecked")
        void deliverErased(Object instance) {
            deliver((T) instance);
        }

        T current() {
            return current;
        }

        void releaseCurrent() {
            release(current);
            current = null;
        }

        private void safeInvoke(T instance) {
            try {
                callback.accept(instance);
            } catch (Throwable t) {
                // A throwing consumer callback must not kill the subscription; later deliveries proceed.
                log.warn().exception(t).log("TLS reload callback threw; subscription stays live");
            }
        }
    }

    /** A one-shot handle: exposes the built instance and releases its retained reference on dispose. */
    private static final class OneShotHandle<T> implements TlsHandle<T> {
        private final T instance;
        private volatile boolean disposed;

        OneShotHandle(T instance) {
            this.instance = instance;
        }

        @Override
        public T get() {
            return instance;
        }

        @Override
        public void dispose() {
            if (!disposed) {
                disposed = true;
                release(instance);
            }
        }
    }

    /** A subscribing handle: exposes the most-recently-delivered instance and unregisters on dispose. */
    private static final class SubscriptionHandle<T> implements TlsHandle<T> {
        private final RegisteredSource source;
        private final Subscription<T> subscription;
        private volatile boolean disposed;

        SubscriptionHandle(RegisteredSource source, Subscription<T> subscription) {
            this.source = source;
            this.subscription = subscription;
        }

        @Override
        public T get() {
            return subscription.current();
        }

        @Override
        public void dispose() {
            if (!disposed) {
                disposed = true;
                source.removeSubscription(subscription);
            }
        }
    }

    /** Thrown when a resolved server purpose has no configured material (a resolved-but-unbuildable case). */
    static final class TlsMaterialUnavailableException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        TlsMaterialUnavailableException(String message) {
            super(message);
        }
    }
}
