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
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/** Owns its broker lifecycle because the test deliberately shuts down the leader. */
@Test(groups = "broker")
public class BrokerLeadershipShutdownTest extends ExtensibleLoadManagerImplBaseTest {
    @Factory(dataProvider = "serviceUnitStateTableViewClassName")
    public BrokerLeadershipShutdownTest(String tableViewClassName) {
        super("public/shutdown-leader", tableViewClassName);
    }

    @Override
    protected BrokerService customizeNewBrokerService(BrokerService service) {
        return mockingDetails(service).isMock() ? service : spy(service);
    }

    @Test
    public void successorIsElectedBeforeBundleDrain() throws Exception {
        PulsarService leader = pulsar1.getLeaderElectionService().isLeader() ? pulsar1 : pulsar2;
        PulsarService successor = leader == pulsar1 ? pulsar2 : pulsar1;
        BrokerService broker = leader.getBrokerService();
        AtomicBoolean draining = new AtomicBoolean();
        doAnswer(invocation -> {
            assertFalse(leader.getLeaderElectionService().isElectionEnabled());
            assertFalse(leader.getLeaderElectionService().isLeader());
            assertTrue(successor.getLeaderElectionService().isLeader());
            draining.set(true);
            return invocation.callRealMethod();
        }).when(broker).unloadNamespaceBundlesGracefully(anyInt(), anyBoolean());
        leader.getConfiguration().setBrokerShutdownTimeoutMs(20000);
        leader.closeAsync().get(30, TimeUnit.SECONDS);
        assertTrue(draining.get());
        assertTrue(successor.getLeaderElectionService().isLeader());
        assertTrue(leader.isMetadataSessionsClosing());
    }
}
