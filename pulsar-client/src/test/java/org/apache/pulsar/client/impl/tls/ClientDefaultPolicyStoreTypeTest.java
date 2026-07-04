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
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.testng.annotations.Test;

/**
 * PIP-478 P1 (v4 parity): {@code clientDefaultPolicy} maps the keystore and truststore types independently.
 * The pre-fix code collapsed them into a single {@code storeType} via a {@code keyStorePath != null} heuristic,
 * silently dropping the truststore type in a mixed setup (e.g. a PKCS12 keystore with a JKS truststore).
 */
public class ClientDefaultPolicyStoreTypeTest {

    @Test
    public void mapsSeparateKeyAndTrustStoreTypes() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setUseKeyStoreTls(true);
        conf.setTlsKeyStorePath("/path/key.p12");
        conf.setTlsKeyStorePassword("kpw");
        conf.setTlsKeyStoreType("PKCS12");
        conf.setTlsTrustStorePath("/path/trust.jks");
        conf.setTlsTrustStorePassword("tpw");
        conf.setTlsTrustStoreType("JKS");

        TlsPolicy policy = ClientTlsFactorySupport.clientDefaultPolicy(conf);

        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.KEYSTORE);
        assertThat(policy.keyStoreType()).as("keystore type mapped independently").isEqualTo("PKCS12");
        assertThat(policy.trustStoreType()).as("truststore type NOT clobbered by the keystore type")
                .isEqualTo("JKS");
        assertThat(policy.keyStorePath()).isEqualTo("/path/key.p12");
        assertThat(policy.trustStorePath()).isEqualTo("/path/trust.jks");
    }

    @Test
    public void trustStoreTypePreservedWhenNoKeyStorePath() {
        // Trust-only keystore config (no client identity): the truststore type must still map — the old
        // heuristic fell back to the truststore type only when the keystore path was null, which happened to
        // work here but broke the moment a keystore path was also present.
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setUseKeyStoreTls(true);
        conf.setTlsTrustStorePath("/path/trust.jks");
        conf.setTlsTrustStorePassword("tpw");
        conf.setTlsTrustStoreType("JKS");

        TlsPolicy policy = ClientTlsFactorySupport.clientDefaultPolicy(conf);

        assertThat(policy.trustStoreType()).isEqualTo("JKS");
        assertThat(policy.keyStorePath()).isNull();
    }
}
