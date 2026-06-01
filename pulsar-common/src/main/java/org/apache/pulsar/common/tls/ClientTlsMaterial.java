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
package org.apache.pulsar.common.tls;

/**
 * Client-side {@link TlsMaterial} (PIP-478).
 */
public interface ClientTlsMaterial extends TlsMaterial {

    /**
     * @return whether the server hostname is verified against the certificate; default {@code true}
     */
    default boolean isHostnameVerificationRequired() {
        return true;
    }

    /**
     * @return whether to trust any CA certificate (insecure mode). Default {@code false}. Setting
     *         {@code true} matches the legacy insecure mode and breaks mTLS.
     */
    default boolean isTrustAnyCaCert() {
        return false;
    }
}
