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
package org.apache.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarBrokerShutdownTest {
    @Test
    public void sigtermClosesBothSessionsBeforeProcessTermination() throws Exception {
        Path directory = Files.createTempDirectory("pulsar-shutdown-");
        String classpath = System.getProperty("java.class.path");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, ShutdownProcess.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(directory.resolve("process.log").toFile()).start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> Files.exists(directory.resolve("ready")));
            process.destroy(); // SIGTERM, including execution of the broker shutdown hook.
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).as("process exits within the shutdown budget").isTrue();
            assertThat(directory.resolve("local-closed")).exists();
            assertThat(directory.resolve("configuration-closed")).exists();
        } finally {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            FileUtils.deleteDirectory(directory.toFile());
        }
    }

    public static class ShutdownProcess extends PulsarService {
        ShutdownProcess(Path directory) throws Exception {
            super(configuration());
            MetadataStoreExtended local = mock(MetadataStoreExtended.class);
            MetadataStoreExtended configuration = mock(MetadataStoreExtended.class);
            doAnswer(invocation -> Files.writeString(directory.resolve("local-closed"), "closed")).when(local).close();
            doAnswer(invocation -> Files.writeString(directory.resolve("configuration-closed"), "closed"))
                    .when(configuration).close();
            setLocalMetadataStore(local);
            setConfigurationMetadataStore(configuration);
            setShouldShutdownConfigurationMetadataStore(true);
            TopicPoliciesService policies = mock(TopicPoliciesService.class);
            doAnswer(invocation -> {
                new CountDownLatch(1).await();
                return null;
            }).when(policies).close();
            setTopicPoliciesService(policies);
        }

        private static ServiceConfiguration configuration() {
            ServiceConfiguration config = new ServiceConfiguration();
            config.setClusterName("shutdown-process-test");
            config.setMetadataStoreUrl("memory:shutdown-process-test");
            config.setBrokerShutdownTimeoutMs(1500);
            return config;
        }

        public static void main(String[] args) throws Exception {
            Path directory = Path.of(args[0]);
            ShutdownProcess service = new ShutdownProcess(directory);
            PulsarBrokerStarter.enforceShutdownDeadline(service);
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> service.closeAsync().handle((__, error) -> null).join()));
            // Even an unrelated blocked hook must not prevent termination after session cleanup.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> new CompletableFuture<Void>().join()));
            Files.writeString(directory.resolve("ready"), "ready");
            new CountDownLatch(1).await();
        }
    }
}
