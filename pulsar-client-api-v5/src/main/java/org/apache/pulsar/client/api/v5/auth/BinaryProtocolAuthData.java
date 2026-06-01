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
 * A binary-protocol credential: the auth-method name and the {@code auth_data} bytes that the
 * framework places in {@code CommandConnect} (PIP-478).
 */
public interface BinaryProtocolAuthData {

    /**
     * @return the auth-method name sent in {@code CommandConnect.auth_method_name}
     */
    String authMethodName();

    /**
     * @return the binary credential bytes sent in {@code CommandConnect.auth_data}
     */
    byte[] authData();
}
