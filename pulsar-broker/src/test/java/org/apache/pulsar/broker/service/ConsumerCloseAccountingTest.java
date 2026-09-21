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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsumerCloseAccountingTest extends SharedPulsarBaseTest {
    @DataProvider
    public Object[][] subscriptionTypes() {
        return new Object[][] {{false, SubscriptionType.Shared}, {false, SubscriptionType.Key_Shared},
                {true, SubscriptionType.Shared}, {true, SubscriptionType.Key_Shared}};
    }

    @Test(dataProvider = "subscriptionTypes")
    public void testLateBrokerCloseDoesNotDebitConsumerTwice(boolean nonPersistent, SubscriptionType type)
            throws Exception {
        String topicName = newTopicName();
        if (nonPersistent) {
            topicName = topicName.replace("persistent://", "non-persistent://");
        }
        try (var clientConsumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("sub")
                .subscriptionType(type).subscribe()) {
            AbstractTopic topic = (AbstractTopic) getTopic(topicName, false).get(5, TimeUnit.SECONDS).orElseThrow();
            Consumer brokerConsumer = topic.getSubscription("sub").getDispatcher().getConsumers().get(0);
            assertThat(topic.currentUsageCount()).isEqualTo(1);
            clientConsumer.close();
            assertThat(topic.currentUsageCount()).isZero();

            // A broker disconnect can have captured this consumer before the client close removed it.
            brokerConsumer.close();
            assertThat(topic.currentUsageCount()).isZero();
        }
    }
}
