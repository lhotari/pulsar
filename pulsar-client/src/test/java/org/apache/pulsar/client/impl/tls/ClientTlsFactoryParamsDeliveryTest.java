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
package org.apache.pulsar.client.impl.tls;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 4b: a custom (non-default) TLS factory named by {@code brokerClientTlsFactoryClassName}
 * receives its {@code brokerClientTlsFactoryConfig} parameters through {@link TlsFactoryInitContext#params()}.
 * The broker parses that config into {@code ClientConfigurationData.tlsFactoryParams}; this asserts the
 * client-side delivery of that map into the init context a resolved factory is initialized with, using a
 * params-capturing fake factory.
 */
public class ClientTlsFactoryParamsDeliveryTest {

    private ScheduledExecutorService executor;

    @BeforeMethod
    public void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void deliversTlsFactoryParamsToTheInitContext() throws Exception {
        ParamsCapturingTlsFactory fake = new ParamsCapturingTlsFactory();
        ClientConfigurationData conf = newTlsConf(fake);
        conf.setTlsFactoryParams(Map.of("issuerUrl", "https://idp.example", "region", "eu"));

        PulsarTlsFactory resolved =
                ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor, null);

        assertThat(resolved).as("the adopted factory is returned as-is").isSameAs(fake);
        assertThat(fake.capturedParams)
                .as("the broker-delivered brokerClientTlsFactoryConfig params reach the custom factory")
                .containsEntry("issuerUrl", "https://idp.example")
                .containsEntry("region", "eu");
    }

    @Test
    public void emptyParamsWhenUnset() throws Exception {
        ParamsCapturingTlsFactory fake = new ParamsCapturingTlsFactory();
        ClientConfigurationData conf = newTlsConf(fake);

        ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor, null);

        assertThat(fake.capturedParams).as("no params delivered => empty map (never null)").isEmpty();
    }

    private static ClientConfigurationData newTlsConf(PulsarTlsFactory factory) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar+ssl://localhost:6651");
        conf.setTlsFactory(factory);
        return conf;
    }

    /** A minimal {@link PulsarTlsFactory} that records the init-context params and serves an empty client context. */
    private static final class ParamsCapturingTlsFactory implements PulsarTlsFactory {
        private volatile Map<String, String> capturedParams;

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            this.capturedParams = context.params();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            if (instanceClass != SslContext.class) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            try {
                SslContext context = SslContextBuilder.forClient().build();
                TlsHandle<T> handle = new TlsHandle<>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public T get() {
                        return (T) context;
                    }

                    @Override
                    public void dispose() {
                    }
                };
                return CompletableFuture.completedFuture(Optional.of(handle));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return createInstance(purpose, instanceClass).thenApply(opt -> {
                opt.ifPresent(handle -> onLoadOrReload.accept(handle.get()));
                return opt;
            });
        }

        @Override
        public void close() {
        }
    }
}
