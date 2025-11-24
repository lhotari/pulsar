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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-replication")
public class OneWayReplicatorSchemaTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Data
    private static class MyClass {
        int field1;
        String field2;
        Long field3;
    }

    @Override
    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        config.setAllowAutoTopicCreation(false);
        config.setSchemaValidationEnforced(true);
    }

    /*
    @Override
    protected void startPulsarClient() throws Exception {
        // use binary protocol for lookups
        ClientBuilder clientBuilder1 = PulsarClient.builder().serviceUrl(pulsar1.getBrokerServiceUrl());
        client1 = initClient(clientBuilder1);
        ClientBuilder clientBuilder2 = PulsarClient.builder().serviceUrl(pulsar2.getBrokerServiceUrl());
        client2 = initClient(clientBuilder2);
    }*/

    @Test(timeOut = 30000)
    public void testPrecreatedTopicsWithAvroSchema() throws Exception {
        Schema<MyClass> myClassSchema = Schema.AVRO(MyClass.class);
        final String topicName =
                BrokerTestUtil.newUniqueName("persistent://" + sourceClusterAlwaysSchemaCompatibleNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.schemas().createSchema(topicName, myClassSchema.getSchemaInfo());
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.schemas().createSchema(topicName, myClassSchema.getSchemaInfo());

        // unloading the topic fixes the issue
        //admin1.topics().unload(topicName);

        Consumer<MyClass> consumer2 = client2.newConsumer(myClassSchema)
                .topic(topicName).subscriptionName("sub").subscribe();

        Producer<byte[]> producer1 = client1.newProducer(Schema.AUTO_PRODUCE_BYTES()).topic(topicName).create();
        MyClass payload = new MyClass();
        payload.setField1(1);
        payload.setField2("test");
        payload.setField3(123456789L);
        byte[] payloadBytes = myClassSchema.encode(payload);
        producer1.send(payloadBytes);

        Message<MyClass> received = consumer2.receive(5, TimeUnit.SECONDS);
        assertThat(received).isNotNull();
        assertThat(received.getValue()).isNotNull().satisfies(body -> {
            assertThat(body.getField1()).isEqualTo(1);
            assertThat(body.getField2()).isEqualTo("test");
            assertThat(body.getField3()).isEqualTo(123456789L);
        });
    }

}
