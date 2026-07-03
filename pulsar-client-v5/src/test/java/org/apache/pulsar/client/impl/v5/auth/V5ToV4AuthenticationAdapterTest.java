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
package org.apache.pulsar.client.impl.v5.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.testng.annotations.Test;

/**
 * Verifies that {@link V5ToV4AuthenticationAdapter} exposes a v5 plugin through the v4
 * {@code Authentication} interface that {@code ClientCnx} drives, and refuses serialization with an
 * actionable message (v5 plugins are deliberately not {@code Serializable}).
 */
public class V5ToV4AuthenticationAdapterTest {

    private static V5ToV4AuthenticationAdapter wrap() {
        return new V5ToV4AuthenticationAdapter(new TlsAuthentication(), null, null, null, "test", Map.of());
    }

    @Test
    public void exposesV5MethodNameAndEmptyCredentialThroughV4Interface() throws Exception {
        V5ToV4AuthenticationAdapter adapter = wrap();
        assertThat(adapter.getAuthMethodName()).isEqualTo("tls");
        // start() drives the v5 initializeAsync(...) — a no-op for the mTLS plugin — and must not throw.
        adapter.start();

        AuthenticationDataProvider data = adapter.getAuthData("broker-1.example.com");
        // mTLS carries an empty binary payload; the certificate is presented at the TLS handshake.
        assertThat(data.getCommandData()).isEmpty();
    }

    @Test
    public void refusesSerializationWithActionableMessage() {
        assertThatThrownBy(() -> new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(wrap()))
                .isInstanceOf(NotSerializableException.class)
                .hasMessageContaining("authPluginClassName");
    }
}
