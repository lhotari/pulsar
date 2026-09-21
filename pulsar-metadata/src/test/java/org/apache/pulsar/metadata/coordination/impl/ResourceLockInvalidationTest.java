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
package org.apache.pulsar.metadata.coordination.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class ResourceLockInvalidationTest extends BaseMetadataStoreTest {
    @Test(dataProvider = "zkImpls")
    public void testFailedReadAfterInvalidationCannotDeleteReplacementWithResetVersion(
            String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended ownerStore = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        @Cleanup
        MetadataStoreExtended replacementStore = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        MetadataStoreExtended observedStore = spy(ownerStore);
        MetadataSerde<String> serde = new MetadataSerde<>() {
            @Override
            public byte[] serialize(String path, String value) {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(String path, byte[] content, Stat stat) {
                return new String(content, StandardCharsets.UTF_8);
            }
        };
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        String path = newKey() + "/bundle";
        try {
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(observedStore, serde, path, executor);
            lock.acquire("old-owner").get(5, TimeUnit.SECONDS);
            long previousVersion = ownerStore.get(path).get(5, TimeUnit.SECONDS).orElseThrow().getStat().getVersion();
            ownerStore.delete(path, Optional.of(previousVersion)).get(5, TimeUnit.SECONDS);
            Stat replacement = replacementStore.put(path, "replacement".getBytes(StandardCharsets.UTF_8),
                    Optional.of(-1L), EnumSet.of(CreateOption.Ephemeral)).get(5, TimeUnit.SECONDS);
            assertThat(replacement.getVersion()).as("ZooKeeper versions restart for a recreated path")
                    .isEqualTo(previousVersion);

            CompletableFuture<Optional<GetResult>> revalidationRead = new CompletableFuture<>();
            doReturn(revalidationRead).when(observedStore).get(path);
            // Deliver the invalidation only after the replacement exists. This is a normal delayed notification,
            // not permission to delete using the old generation's version when its verification read fails.
            lock.lockWasInvalidated();
            CompletableFuture<Void> released = lock.release();
            revalidationRead.completeExceptionally(new MetadataStoreException("revalidation read failed"));
            released.handle((__, error) -> null).get(5, TimeUnit.SECONDS);
            assertThat(replacementStore.get(path).get(5, TimeUnit.SECONDS))
                    .as("a failed non-mutating read cannot prove the old generation still owns a recreated node")
                    .hasValueSatisfying(value -> assertThat(new String(value.getValue(), StandardCharsets.UTF_8))
                            .isEqualTo("replacement"));
            assertThatThrownBy(() -> released.get(5, TimeUnit.SECONDS)).hasRootCauseMessage("revalidation read failed");
            assertThatThrownBy(() -> lock.release().get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("revalidation read failed");
            verify(observedStore, never()).delete(anyString(), any());
        } finally {
            executor.shutdownNow();
        }
    }
}
