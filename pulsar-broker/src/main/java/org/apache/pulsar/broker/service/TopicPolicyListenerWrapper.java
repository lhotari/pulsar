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

package org.apache.pulsar.broker.service;

import org.apache.pulsar.common.policies.data.TopicPolicies;

/**
 * This TopicPolicyListener is used as a wrapper for the real TopicPolicyListener.
 * This prevents a race condition in initialization where the topic policy state can change while the
 * topic policy state is being applied to the topic in PersistentTopic#initTopicPolicy() method or in
 * NonPersistentTopic#initialize method. The impact of the race conditions is that the topic policy state would
 * be left in an inconsistent state until another update arrives. This is a rare corner case, but possible.
 */
public class TopicPolicyListenerWrapper implements TopicPolicyListener {
    private final TopicPolicyListener realTopicListener;
    private TopicPolicies latestGlobalPolicies;
    private TopicPolicies latestLocalPolicies;
    private boolean initialized;

    public TopicPolicyListenerWrapper(TopicPolicyListener realTopicListener) {
        this.realTopicListener = realTopicListener;
    }

    @Override
    public synchronized void onUpdate(TopicPolicies data) {
        if (initialized) {
            realTopicListener.onUpdate(data);
        } else {
            if (data.isGlobalPolicies()) {
                latestGlobalPolicies = data;
            } else {
                latestLocalPolicies = data;
            }
        }
    }

    /**
     * Complete initialization of the TopicPolicyListenerWrapper and emit the latest policies to the real listener.
     * @param loadedGlobalPolicies the loaded global policies
     * @param loadedLocalPolicies the loaded local policies
     */
    public synchronized void completeInitialization(TopicPolicies loadedGlobalPolicies,
                                                    TopicPolicies loadedLocalPolicies) {
        // the listener might have received a newer value than the loaded one while the loading was happening
        // use the latest values received by the listener, fallback to use the loaded values

        if (latestGlobalPolicies != null) {
            realTopicListener.onUpdate(latestGlobalPolicies);
            latestGlobalPolicies = null;
        } else if (loadedGlobalPolicies != null) {
            realTopicListener.onUpdate(loadedGlobalPolicies);
        }

        if (latestLocalPolicies != null) {
            realTopicListener.onUpdate(latestLocalPolicies);
            latestLocalPolicies = null;
        } else if (loadedLocalPolicies != null) {
            realTopicListener.onUpdate(loadedLocalPolicies);
        }

        initialized = true;
    }
}
