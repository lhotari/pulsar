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
package org.apache.pulsar.broker.tls;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.handler.ssl.SslProvider;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Shared scaffolding for wiring a component onto the PIP-478 {@link PulsarTlsFactory} SPI alongside the
 * still-functional PIP-337 {@code PulsarSslFactory} path (stage 2b). Server components (broker, proxy,
 * websocket, functions-worker) call these helpers to decide which TLS path to use, instantiate and
 * initialize the new factory, and parse its parameters.
 *
 * <p>The helper is intentionally free of Netty {@code io.netty.handler.ssl} types (the {@code SslContext}
 * subscribe pattern stays inline in the binary-listener components that already depend on
 * {@code netty-handler}); it carries only {@link SslProvider} from {@code netty-common}, which
 * {@code pulsar-broker-common} already has. That keeps this class usable by every component, including the
 * websocket proxy and functions-worker web servers whose only TLS consumer is Jetty.
 *
 * <p><b>Selection rule (EXPERIMENT — default flipped to the new factory).</b> {@link #selectPath} chooses
 * the legacy PIP-337 path or the new PIP-478 path:
 * <ol>
 *   <li>a non-default {@code sslFactoryPlugin}-family value keeps the legacy PIP-337 path (a deprecation
 *       warning is logged once per distinct plugin class);</li>
 *   <li>otherwise, a non-blank {@code tlsFactoryClassName}-family value selects the new PIP-478 path;</li>
 *   <li>otherwise (both default/blank) the new PIP-478 built-in factory is used.</li>
 * </ol>
 * This is the {@code lh-pip-478-tls-default-flip} experiment branch: the default is flipped so a blank
 * {@code tlsFactoryClassName} selects {@link #createFactory the built-in composed default factory} instead
 * of the legacy PIP-337 default. On the mainline {@code lh-pip-478-impl-v2} the new path is opt-in (this
 * last case returns {@code LEGACY}); the flip is the single-line change that stage 4 (PIP-337 removal) makes
 * permanent. Only a custom {@code sslFactoryPlugin} still selects the legacy path.
 */
@CustomLog
public final class TlsFactorySupport {

    /**
     * Reserved {@code tlsFactoryClassName} value selecting the component's built-in default
     * {@link PulsarTlsFactory} (composed from the component configuration) via the new SPI, rather than a
     * reflectively-instantiated custom factory.
     */
    public static final String DEFAULT_FACTORY = "default";

    /**
     * FQCN of the removed PIP-337 default SSL factory. Matched as a string literal (the class itself is
     * removed in PIP-478 stage 4c) so a blank {@code sslFactoryPlugin} value OR this literal is treated as
     * "the default" — any other non-blank value names a custom PIP-337 plugin.
     */
    static final String REMOVED_DEFAULT_SSL_FACTORY_CLASS_NAME =
            "org.apache.pulsar.common.util.DefaultPulsarSslFactory";

    private static final Map<String, Boolean> LEGACY_WARNED = new ConcurrentHashMap<>();

    private TlsFactorySupport() {
    }

    /** Which TLS integration path a component uses for a given configuration. */
    public enum TlsPath {
        /** The PIP-337 {@code PulsarSslFactory} path (unchanged legacy behavior). */
        LEGACY,
        /** The PIP-478 {@link PulsarTlsFactory} path. */
        NEW
    }

    /**
     * Decide the TLS path per the selection rule documented on this class. When the legacy path is chosen
     * because a non-default {@code sslFactoryPlugin} is configured, a deprecation warning is logged once per
     * distinct plugin class name.
     *
     * @param legacyPluginClassName the component's {@code sslFactoryPlugin}-family value (may be null/blank)
     * @param tlsFactoryClassName   the component's {@code tlsFactoryClassName}-family value (may be null/blank)
     * @return {@link TlsPath#LEGACY} or {@link TlsPath#NEW}
     */
    public static TlsPath selectPath(String legacyPluginClassName, String tlsFactoryClassName) {
        if (isLegacyCustom(legacyPluginClassName)) {
            if (LEGACY_WARNED.putIfAbsent(legacyPluginClassName.trim(), Boolean.TRUE) == null) {
                log.warn().attr("sslFactoryPlugin", legacyPluginClassName.trim()).log(
                        "Using the deprecated PIP-337 SSL factory plugin. It is superseded by the PIP-478 "
                                + "tlsFactoryClassName and will be removed in a later release; migrate the "
                                + "plugin to org.apache.pulsar.common.tls.PulsarTlsFactory.");
            }
            return TlsPath.LEGACY;
        }
        if (StringUtils.isNotBlank(tlsFactoryClassName)) {
            return TlsPath.NEW;
        }
        // EXPERIMENT (default flip): both default/blank -> the new PIP-478 built-in factory instead of the
        // legacy PIP-337 default. Each NEW branch resolves a blank tlsFactoryClassName to its component's
        // composed default factory (see createFactory). Flipping this single return flips every component
        // (broker binary+web, proxy PROXY/WEB/BROKER_CLIENT, websocket, functions-worker). Only a custom
        // sslFactoryPlugin still selects the legacy path (above).
        return TlsPath.NEW;
    }

    /**
     * @param legacyPluginClassName a {@code sslFactoryPlugin}-family value
     * @return whether it names a non-default (custom) PIP-337 factory
     */
    public static boolean isLegacyCustom(String legacyPluginClassName) {
        return StringUtils.isNotBlank(legacyPluginClassName)
                && !REMOVED_DEFAULT_SSL_FACTORY_CLASS_NAME.equals(legacyPluginClassName.trim());
    }

    /**
     * Instantiate the PIP-478 factory for the new path. A blank value, the literal {@link #DEFAULT_FACTORY},
     * or the default factory's own class name selects the supplied built-in default (composed from the
     * component configuration); any other value is instantiated reflectively via its public no-arg
     * constructor.
     *
     * @param tlsFactoryClassName the configured factory class name (or blank/{@code default})
     * @param defaultFactoryClass the class of the built-in default factory (for name matching); may be null
     * @param defaultFactory      supplies the built-in default factory
     * @return an uninitialized {@link PulsarTlsFactory} (call {@link #initializeBlocking} before use)
     * @throws ReflectiveOperationException if a named custom class cannot be instantiated
     */
    public static PulsarTlsFactory createFactory(String tlsFactoryClassName,
                                                 Class<? extends PulsarTlsFactory> defaultFactoryClass,
                                                 Supplier<PulsarTlsFactory> defaultFactory)
            throws ReflectiveOperationException {
        Objects.requireNonNull(defaultFactory, "defaultFactory must not be null");
        String className = tlsFactoryClassName == null ? "" : tlsFactoryClassName.trim();
        if (className.isEmpty()
                || DEFAULT_FACTORY.equalsIgnoreCase(className)
                || (defaultFactoryClass != null && defaultFactoryClass.getName().equals(className))) {
            return defaultFactory.get();
        }
        return (PulsarTlsFactory) Class.forName(className).getConstructor().newInstance();
    }

    /**
     * Build a production {@link TlsFactoryInitContext} with a no-op {@link OpenTelemetry}. For components
     * (websocket proxy, functions worker) whose module does not carry {@code opentelemetry-api} on its
     * compile classpath and only need TLS for the Jetty web path.
     *
     * @param params           the factory params (from {@link #parseFactoryConfig}); never null
     * @param scheduler        the framework scheduler for file-watch polling and rotation
     * @param blockingExecutor the executor for potentially-blocking material loading
     * @return a {@link TlsFactoryInitContext} with {@link OpenTelemetry#noop()}
     */
    public static TlsFactoryInitContext initContext(Map<String, String> params,
                                                    ScheduledExecutorService scheduler,
                                                    Executor blockingExecutor) {
        return initContext(params, scheduler, blockingExecutor, OpenTelemetry.noop());
    }

    /**
     * Build a production {@link TlsFactoryInitContext}.
     *
     * @param params           the factory params (from {@link #parseFactoryConfig}); never null
     * @param scheduler        the framework scheduler for file-watch polling and rotation
     * @param blockingExecutor the executor for potentially-blocking material loading
     * @param openTelemetry    the telemetry root, or {@code null} for {@link OpenTelemetry#noop()}
     * @return a {@link TlsFactoryInitContext}
     */
    public static TlsFactoryInitContext initContext(Map<String, String> params,
                                                    ScheduledExecutorService scheduler,
                                                    Executor blockingExecutor,
                                                    OpenTelemetry openTelemetry) {
        Map<String, String> safeParams = params == null ? Map.of() : Map.copyOf(params);
        OpenTelemetry ot = openTelemetry == null ? OpenTelemetry.noop() : openTelemetry;
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return safeParams;
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return scheduler;
            }

            @Override
            public Executor blockingExecutor() {
                return blockingExecutor;
            }

            @Override
            public Clock clock() {
                return Clock.systemUTC();
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return ot;
            }
        };
    }

    /**
     * Initialize a factory and block until it is ready, per the fail-fast contract (a failed
     * {@code initialize} is fatal to the owning component's startup). Unwraps
     * {@link CompletionException}/{@link ExecutionException} to the underlying cause.
     *
     * @param factory the factory to initialize
     * @param context the init context
     * @throws Exception the underlying initialization failure, if any
     */
    public static void initializeBlocking(PulsarTlsFactory factory, TlsFactoryInitContext context)
            throws Exception {
        try {
            factory.initialize(context).get();
        } catch (ExecutionException e) {
            throw asException(e.getCause());
        } catch (CompletionException e) {
            throw asException(e.getCause());
        }
    }

    /**
     * Parse a {@code tlsFactoryConfig} string into the factory params map. A blank value yields an empty
     * map; a value starting with <code>{</code> is parsed as a JSON object; otherwise it is parsed as a
     * comma-separated {@code key=value} list.
     *
     * @param tlsFactoryConfig the configured factory params (may be null/blank)
     * @return an immutable params map (possibly empty)
     */
    public static Map<String, String> parseFactoryConfig(String tlsFactoryConfig) {
        if (StringUtils.isBlank(tlsFactoryConfig)) {
            return Map.of();
        }
        String trimmed = tlsFactoryConfig.trim();
        if (trimmed.startsWith("{")) {
            try {
                Map<String, String> parsed = ObjectMapperFactory.getMapper().reader()
                        .forType(new TypeReference<Map<String, String>>() {})
                        .readValue(trimmed);
                return parsed == null ? Map.of() : Map.copyOf(parsed);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse tlsFactoryConfig as a JSON object", e);
            }
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (String pair : trimmed.split(",")) {
            String entry = pair.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int eq = entry.indexOf('=');
            if (eq < 0) {
                map.put(entry, "");
            } else {
                map.put(entry.substring(0, eq).trim(), entry.substring(eq + 1).trim());
            }
        }
        return Map.copyOf(map);
    }

    /**
     * Map a component's provider string to the Netty {@link SslProvider} engine used by the default
     * file-based factory. Conservative: only an explicit {@code OPENSSL}/{@code OPENSSL_REFCNT} value selects
     * the native engine — JCE provider names (e.g. {@code Conscrypt}, {@code SunJSSE}) and {@code null} map
     * to the {@link SslProvider#JDK} engine (the safe default). The provider string is still passed
     * separately to Jetty as its JCE provider for the web path.
     *
     * @param providerString the component's provider string (may be null/blank)
     * @return the Netty {@link SslProvider} engine selection
     */
    public static SslProvider engineProvider(String providerString) {
        if (StringUtils.isNotBlank(providerString)) {
            String provider = providerString.trim();
            if ("OPENSSL".equalsIgnoreCase(provider) || "OPENSSL_REFCNT".equalsIgnoreCase(provider)) {
                return SslProvider.OPENSSL;
            }
        }
        return SslProvider.JDK;
    }

    private static Exception asException(Throwable cause) {
        if (cause == null) {
            return new Exception("TLS factory initialization failed");
        }
        if (cause instanceof Exception e) {
            return e;
        }
        return new Exception(cause);
    }
}
