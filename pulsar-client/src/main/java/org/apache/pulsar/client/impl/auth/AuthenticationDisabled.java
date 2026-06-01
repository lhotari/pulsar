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
package org.apache.pulsar.client.impl.auth;

import java.io.IOException;
import java.util.Map;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.v5.DisabledAuthenticationV5;

/**
 * Authentication provider for the {@code none} auth method (no credential).
 *
 * <p>PIP-478: this v4 plugin is a thin shim over the v5-native {@link DisabledAuthenticationV5}. The
 * v4 surface (including the {@link #INSTANCE} singleton and {@link #getAuthData()} returning
 * {@link AuthenticationDataNull}) is preserved for source compatibility.
 *
 * <p>Unlike credential plugins, the {@code none} method does no credential I/O, so it intentionally
 * does NOT implement {@code AsyncAuthenticationDriver}: {@code ClientCnx} drives it through the
 * verbatim synchronous connect path, which is the most common no-auth case and avoids needlessly
 * scheduling onto the event-loop executor.
 */
public class AuthenticationDisabled implements Authentication, EncodedAuthenticationParameterSupport {

    protected final AuthenticationDataProvider nullData = new AuthenticationDataNull();
    public static final AuthenticationDisabled INSTANCE = new AuthenticationDisabled();
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public AuthenticationDisabled() {
    }

    @Override
    public String getAuthMethodName() {
        return DisabledAuthenticationV5.AUTH_METHOD_NAME;
    }

    @SuppressWarnings("deprecation")
    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        return nullData;
    }

    @Override
    public void configure(String encodedAuthParamString) {
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
    }

    @Override
    public void start() throws PulsarClientException {
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }
}
