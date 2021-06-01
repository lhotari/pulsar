/**
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
package org.apache.pulsar.metadata.impl;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.zookeeper.AsyncCallback;

/**
 * Decorates Zookeeper Client callbacks to prevent blocking the Zookeeper client's event thread.
 */
public class ZooKeeperCallbackExecutor {
    private final OrderedExecutor executor;

    public ZooKeeperCallbackExecutor(OrderedExecutor executor) {
        this.executor = executor;
    }

    public AsyncCallback.DataCallback decorateDataCallback(AsyncCallback.DataCallback callback) {
        return (rc, path, ctx, data, stat) -> executor.executeOrdered(path, () -> callback
                .processResult(rc, path, ctx, data, stat));
    }

    public AsyncCallback.StatCallback decorateStatCallback(AsyncCallback.StatCallback callback) {
        return (rc, path, ctx, stat) -> executor.executeOrdered(path, () -> callback
                .processResult(rc, path, ctx, stat));
    }

    public AsyncCallback.ChildrenCallback decorateChildrenCallback(AsyncCallback.ChildrenCallback
                                                                           callback) {
        return (rc, path, ctx, children) -> executor.executeOrdered(path, () -> callback
                .processResult(rc, path, ctx, children));
    }

    public AsyncCallback.VoidCallback decorateVoidCallback(AsyncCallback.VoidCallback callback) {
        return (rc, path, ctx) -> executor.executeOrdered(path, () -> callback.processResult(rc, path, ctx));
    }

    public AsyncCallback.StringCallback decorateStringCallback(AsyncCallback.StringCallback callback) {
        return (rc, path, ctx, name) -> executor.executeOrdered(path, () -> callback
                .processResult(rc, path, ctx, name));
    }
}