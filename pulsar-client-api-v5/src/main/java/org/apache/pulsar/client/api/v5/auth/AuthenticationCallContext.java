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
 * The per-call context for binary-protocol authentication (PIP-478).
 *
 * <p>Carries the broker routing details and a per-exchange state slot (inherited from
 * {@link StatefulCallContext}). Most implementations produce the same credential for every call and
 * never inspect the routing fields.
 */
public interface AuthenticationCallContext extends StatefulCallContext {

    /**
     * @return the broker host this connection is being established to
     */
    String brokerHost();

    /**
     * @return the broker port this connection is being established to
     */
    int brokerPort();
}
