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
package org.apache.pulsar.common.util.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.security.Provider;
import org.testng.annotations.Test;

/**
 * Unit tests for the PIP-478 {@link JcaProviders#resolveNamedProvider(String)} JCA-provider resolution.
 */
public class JcaProvidersTest {

    @Test
    public void blankNameResolvesToNull() {
        assertThat(JcaProviders.resolveNamedProvider(null)).isNull();
        assertThat(JcaProviders.resolveNamedProvider("")).isNull();
        assertThat(JcaProviders.resolveNamedProvider("   ")).isNull();
    }

    @Test
    public void resolvesAnAlreadyRegisteredProvider() {
        // SunJSSE is a built-in provider registered in every JVM; it is found via the Security.getProvider
        // fallback (not ServiceLoader, which only surfaces META-INF/services-declared providers).
        Provider provider = JcaProviders.resolveNamedProvider("SunJSSE");
        assertThat(provider).isNotNull();
        assertThat(provider.getName()).isEqualTo("SunJSSE");
    }

    @Test
    public void unresolvableNameFailsLoud() {
        assertThatThrownBy(() -> JcaProviders.resolveNamedProvider("NoSuchCryptoProvider_pip478"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NoSuchCryptoProvider_pip478");
    }
}
