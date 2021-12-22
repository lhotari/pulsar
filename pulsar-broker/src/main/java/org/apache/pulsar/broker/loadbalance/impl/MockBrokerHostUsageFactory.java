/**
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
package org.apache.pulsar.broker.loadbalance.impl;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsageFactory;

@Slf4j
public class MockBrokerHostUsageFactory implements BrokerHostUsageFactory {
    static final String ENV_NAME_MOCK_BROKER_USAGE_FILE = "PULSAR_MOCK_BROKERHOST_USAGE_FILE";

    public BrokerHostUsage createBrokerHostUsage(PulsarService pulsar) {
        File resourceUsageFile;
        String mockBrokerUsageFile = System.getenv(ENV_NAME_MOCK_BROKER_USAGE_FILE);
        if (mockBrokerUsageFile != null) {
            resourceUsageFile = new File(mockBrokerUsageFile);
        } else {
            try {
                resourceUsageFile = File.createTempFile("pulsar.mock_brokerhost_usage", ".json");
                resourceUsageFile.delete();
            } catch (IOException e) {
                log.error("Unable to create temp file", e);
                throw new UncheckedIOException(e);
            }
        }
        log.info("Using mocked broker host usage from file {}", resourceUsageFile.getAbsolutePath());
        return new MockBrokerHostUsageImpl(resourceUsageFile, pulsar.getLoadManagerExecutor());
    }
}
