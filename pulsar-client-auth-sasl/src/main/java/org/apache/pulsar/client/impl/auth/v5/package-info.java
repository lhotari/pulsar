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

/**
 * v5-native body for the SASL authentication plugin (PIP-478). The v4 {@code AuthenticationSasl}
 * plugin is a thin shim over {@link org.apache.pulsar.client.impl.auth.v5.SaslAuthenticationV5} for
 * the binary protocol; the HTTP SASL multi-round loop remains in the v4 plugin.
 */
package org.apache.pulsar.client.impl.auth.v5;
