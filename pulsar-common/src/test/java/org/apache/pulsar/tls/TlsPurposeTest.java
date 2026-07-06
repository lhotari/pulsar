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
package org.apache.pulsar.tls;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Locks the freeze-forever {@link TlsPurpose} identity contract (PIP-478): equality is over
 * {@code (role, name)} only, with the fallback treated as resolution metadata, so a purpose resolves to
 * the same purpose→policy map slot regardless of the fallback the caller minted it with. Also covers the
 * {@code server(name, fallback)} overload.
 */
public class TlsPurposeTest {

    @Test
    public void equalsAndHashCodeIgnoreFallback() {
        TlsPurpose noFallback = TlsPurpose.client("x");
        TlsPurpose withFallback = TlsPurpose.client("x", TlsPurpose.CLIENT_DEFAULT);
        TlsPurpose withOtherFallback = TlsPurpose.client("x", TlsPurpose.CLIENT_OAUTH2);

        assertThat(withFallback).isEqualTo(noFallback).isEqualTo(withOtherFallback);
        assertThat(withFallback.hashCode()).isEqualTo(noFallback.hashCode());
    }

    @Test
    public void purposeToPolicyMapResolvesSameSlotRegardlessOfFallback() {
        // The exact defect this guards against: minting with a fallback must NOT split one config entry
        // into two distinct map keys and cause silent lookup misses.
        Map<TlsPurpose, String> map = new HashMap<>();
        map.put(TlsPurpose.client("x"), "policy");

        assertThat(map).containsKey(TlsPurpose.client("x", TlsPurpose.CLIENT_DEFAULT));
        assertThat(map.get(TlsPurpose.client("x", TlsPurpose.CLIENT_OAUTH2))).isEqualTo("policy");
    }

    @Test
    public void roleAndNameStillDistinguish() {
        assertThat(TlsPurpose.client("x")).isNotEqualTo(TlsPurpose.server("x"));
        assertThat(TlsPurpose.client("x")).isNotEqualTo(TlsPurpose.client("y"));
    }

    @Test
    public void serverWithFallbackKeepsFallbackAsMetadataNotIdentity() {
        TlsPurpose internal = TlsPurpose.server("broker.internal", TlsPurpose.BROKER);
        assertThat(internal.role()).isEqualTo(TlsPurpose.Role.SERVER);
        assertThat(internal.name()).isEqualTo("broker.internal");
        assertThat(internal.fallback()).contains(TlsPurpose.BROKER);
        // Same identity as the fallback-less server purpose of the same name.
        assertThat(internal).isEqualTo(TlsPurpose.server("broker.internal"));
    }
}
