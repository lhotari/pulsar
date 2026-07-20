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
package org.apache.pulsar.broker;

import static org.testng.Assert.assertEquals;
import lombok.Cleanup;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

/**
 * PIP-478 (FIX): the broker's internal outbound client ({@code createClientConfigurationData}) must carry the
 * broker-client TLS engine ({@code brokerClientSslProvider}) and JSSE (SSLContext) provider
 * ({@code brokerClientJsseProvider}) config onto the {@link ClientConfigurationData}. They were otherwise dropped
 * (never copied into the config), silently defaulting the engine/provider on the broker's replication/lookup
 * client.
 */
public class PulsarServiceCreateClientConfigTest {

    @Test
    public void carriesBrokerClientTlsProvidersIntoInternalClientConfig() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setBrokerClientTlsEnabled(true);
        config.setBrokerClientSslProvider("OPENSSL");
        config.setBrokerClientJsseProvider("BCJSSE");

        @Cleanup
        PulsarService pulsarService = new PulsarService(config);
        ClientConfigurationData conf = pulsarService.createClientConfigurationData();

        assertEquals(conf.getSslProvider(), "OPENSSL", "broker-client TLS engine propagated");
        assertEquals(conf.getJsseProvider(), "BCJSSE", "broker-client JSSE provider propagated");
    }
}
