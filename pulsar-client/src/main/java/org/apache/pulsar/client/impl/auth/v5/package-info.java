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
 * v5-native implementations of the built-in authentication plugins (PIP-478).
 *
 * <p>Each built-in v4 plugin in {@link org.apache.pulsar.client.impl.auth} is a thin shim over the
 * corresponding v5-native body here, which implements the
 * {@link org.apache.pulsar.client.api.v5.auth.Authentication} SPI. The shared per-call / init context
 * helpers live in {@link org.apache.pulsar.client.impl.auth.v5.V5AuthContexts}.
 */
package org.apache.pulsar.client.impl.auth.v5;
