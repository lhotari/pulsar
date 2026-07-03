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

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.handler.ssl.SslProvider;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;
import org.testng.annotations.Test;

/**
 * Unit tests for the PIP-478 stage-2b {@link TlsFactorySupport} selection rule and helpers.
 */
public class TlsFactorySupportTest {

    private static final String DEFAULT_PLUGIN = DefaultPulsarSslFactory.class.getName();

    @Test
    public void customSslFactoryPluginSelectsLegacy() {
        // A non-default sslFactoryPlugin keeps the legacy PIP-337 path even if tlsFactoryClassName is set.
        assertThat(TlsFactorySupport.selectPath("com.example.CustomSslFactory", ""))
                .isEqualTo(TlsFactorySupport.TlsPath.LEGACY);
        assertThat(TlsFactorySupport.selectPath("com.example.CustomSslFactory", "com.example.MyTlsFactory"))
                .isEqualTo(TlsFactorySupport.TlsPath.LEGACY);
        assertThat(TlsFactorySupport.isLegacyCustom("com.example.CustomSslFactory")).isTrue();
        assertThat(TlsFactorySupport.isLegacyCustom(DEFAULT_PLUGIN)).isFalse();
        assertThat(TlsFactorySupport.isLegacyCustom("")).isFalse();
        assertThat(TlsFactorySupport.isLegacyCustom(null)).isFalse();
    }

    @Test
    public void tlsFactoryClassNameSelectsNew() {
        // Default (or blank) sslFactoryPlugin + a set tlsFactoryClassName selects the new PIP-478 path.
        assertThat(TlsFactorySupport.selectPath(DEFAULT_PLUGIN, "com.example.MyTlsFactory"))
                .isEqualTo(TlsFactorySupport.TlsPath.NEW);
        assertThat(TlsFactorySupport.selectPath("", TlsFactorySupport.DEFAULT_FACTORY))
                .isEqualTo(TlsFactorySupport.TlsPath.NEW);
        assertThat(TlsFactorySupport.selectPath(null, "com.example.MyTlsFactory"))
                .isEqualTo(TlsFactorySupport.TlsPath.NEW);
    }

    @Test
    public void neitherSelectsLegacyDefault() {
        // Both default/blank: the legacy default PIP-337 path is kept unchanged (opt-in for stage 2b).
        assertThat(TlsFactorySupport.selectPath(DEFAULT_PLUGIN, "")).isEqualTo(TlsFactorySupport.TlsPath.LEGACY);
        assertThat(TlsFactorySupport.selectPath("", "")).isEqualTo(TlsFactorySupport.TlsPath.LEGACY);
        assertThat(TlsFactorySupport.selectPath(null, null)).isEqualTo(TlsFactorySupport.TlsPath.LEGACY);
    }

    @Test
    public void createFactoryUsesDefaultForBlankSentinelOrDefaultClassName() throws Exception {
        PulsarTlsFactory sentinel = new NoOpTlsFactory();
        assertThat(TlsFactorySupport.createFactory("", NoOpTlsFactory.class, () -> sentinel)).isSameAs(sentinel);
        assertThat(TlsFactorySupport.createFactory("default", NoOpTlsFactory.class, () -> sentinel))
                .isSameAs(sentinel);
        assertThat(TlsFactorySupport.createFactory("DEFAULT", NoOpTlsFactory.class, () -> sentinel))
                .isSameAs(sentinel);
        assertThat(TlsFactorySupport.createFactory(NoOpTlsFactory.class.getName(), NoOpTlsFactory.class,
                () -> sentinel)).isSameAs(sentinel);
    }

    @Test
    public void createFactoryInstantiatesNamedCustomClassReflectively() throws Exception {
        PulsarTlsFactory sentinel = new NoOpTlsFactory();
        PulsarTlsFactory created =
                TlsFactorySupport.createFactory(NoOpTlsFactory.class.getName(), null, () -> sentinel);
        // With no defaultFactoryClass to match, the class name is instantiated reflectively (a new instance).
        assertThat(created).isInstanceOf(NoOpTlsFactory.class).isNotSameAs(sentinel);
    }

    @Test
    public void parseFactoryConfigHandlesBlankJsonAndKeyValue() {
        assertThat(TlsFactorySupport.parseFactoryConfig("")).isEmpty();
        assertThat(TlsFactorySupport.parseFactoryConfig(null)).isEmpty();
        assertThat(TlsFactorySupport.parseFactoryConfig("{\"a\":\"1\",\"b\":\"2\"}"))
                .containsEntry("a", "1").containsEntry("b", "2");
        assertThat(TlsFactorySupport.parseFactoryConfig("a=1, b = 2 ,c="))
                .containsEntry("a", "1").containsEntry("b", "2").containsEntry("c", "");
    }

    @Test
    public void engineProviderMapsOnlyExplicitOpenSsl() {
        assertThat(TlsFactorySupport.engineProvider(null)).isEqualTo(SslProvider.JDK);
        assertThat(TlsFactorySupport.engineProvider("Conscrypt")).isEqualTo(SslProvider.JDK);
        assertThat(TlsFactorySupport.engineProvider("SunJSSE")).isEqualTo(SslProvider.JDK);
        assertThat(TlsFactorySupport.engineProvider("openssl")).isEqualTo(SslProvider.OPENSSL);
        assertThat(TlsFactorySupport.engineProvider("OPENSSL_REFCNT")).isEqualTo(SslProvider.OPENSSL);
    }

    @Test
    public void initContextWiresServicesAndDefaultsOpenTelemetry() {
        TlsFactoryInitContext ctx = TlsFactorySupport.initContext(java.util.Map.of("k", "v"), null, null, null);
        assertThat(ctx.params()).containsEntry("k", "v");
        assertThat(ctx.clock()).isNotNull();
        assertThat(ctx.openTelemetry()).isSameAs(OpenTelemetry.noop());
    }

    /** A public no-arg {@link PulsarTlsFactory} for reflective-instantiation testing. */
    public static final class NoOpTlsFactory implements PulsarTlsFactory {
        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass,
                                                                            Consumer<T> onLoadOrReload) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public void close() {
        }
    }
}
