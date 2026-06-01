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
package org.apache.pulsar.client.api.v5.auth;

/**
 * Immutable default implementation of {@link BinaryProtocolAuthData} (PIP-478).
 *
 * @param authMethodName the auth-method name
 * @param authData       the credential bytes
 */
public record DefaultBinaryProtocolAuthData(String authMethodName, byte[] authData)
        implements BinaryProtocolAuthData {

    /**
     * Convenience for an empty credential (used by mTLS, which carries no {@code auth_data}).
     *
     * @param authMethodName the auth-method name
     * @return auth data with an empty payload
     */
    public static DefaultBinaryProtocolAuthData empty(String authMethodName) {
        return new DefaultBinaryProtocolAuthData(authMethodName, new byte[0]);
    }
}
