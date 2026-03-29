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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.testng.annotations.Test;

public class ServiceConfigurationUtilsTest {

    @Test
    public void testTrailingDotAddedToResolvedLocalHostFQDN() throws UnknownHostException {
        String fqdnAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(null, true);
        assertTrue(fqdnAddress.endsWith("."));
        // check that we can resolve the fqdn address with the trailing dot
        InetAddress.getByName(fqdnAddress);
    }

    @Test
    public void testTrailingDotNotAddedToResolvedLocalHostFQDNWhenNotRequested() throws UnknownHostException {
        String address = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(null, false);
        assertFalse(address.endsWith("."));
    }

    @Test
    public void testNoTrailingDotAddedIfAddressGiven() {
        String address = ServiceConfigurationUtils.getDefaultOrConfiguredAddress("broker.local", false);
        assertFalse(address.endsWith("."));
    }
}