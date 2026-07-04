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
package org.apache.pulsar.client.api.v5;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import lombok.Cleanup;
import org.apache.pulsar.client.api.TlsProducerConsumerBase;
import org.apache.pulsar.client.api.v5.auth.AuthenticationFactory;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.testng.annotations.Test;

/**
 * End-to-end mTLS produce/consume over the v5 client's new PIP-478 TLS path (stage 3b): a v5
 * {@link PulsarClient} configured with {@code tlsPolicy(...)} against a TLS broker that requires a trusted
 * client certificate. The binary transport builds its per-connection {@code SslContext} through the
 * client-side {@code FileBasedTlsFactory}, not the legacy {@code PulsarSslFactory}.
 */
@Test(groups = "broker-api")
public class V5TlsProducerConsumerTest extends TlsProducerConsumerBase {

    private static final String TOPIC = "persistent://my-property/my-ns/v5-tls-topic";

    /** mTLS material supplied directly through {@link PulsarClientBuilder#tlsPolicy(TlsPolicy)}. */
    @Test
    public void testV5MtlsViaTlsPolicy() throws Exception {
        internalSetUpForNamespace();

        TlsPolicy policy = TlsPolicy.builder()
                .format(TlsPolicy.Format.PEM)
                .trustCertsFilePath(CA_CERT_FILE_PATH)
                .certificateFilePath(getTlsFileForClient("admin.cert"))
                .keyFilePath(getTlsFileForClient("admin.key-pk8"))
                // The shared test broker cert is not guaranteed to carry a SAN for the advertised host, so
                // (as the v4 TLS suites do) hostname verification is left off; the new path bakes trust and
                // the client certificate into the factory-built context.
                .enableHostnameVerification(false)
                .build();

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrlTls())
                .tlsPolicy(policy)
                .build();

        assertProduceConsume(client, TOPIC + "-policy", "v5-mtls-via-policy");
    }

    /**
     * mTLS trust supplied through {@code tlsPolicy(...)} and the client identity through a bridged v4
     * {@code AuthenticationTls} — exercising the builder-time material fold that merges the auth plugin's
     * certificate/key into the {@code CLIENT_DEFAULT} policy on the new path.
     */
    @Test
    public void testV5MtlsViaAuthenticationTlsFold() throws Exception {
        internalSetUpForNamespace();

        TlsPolicy trustOnly = TlsPolicy.builder()
                .format(TlsPolicy.Format.PEM)
                .trustCertsFilePath(CA_CERT_FILE_PATH)
                .enableHostnameVerification(false)
                .build();

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrlTls())
                .tlsPolicy(trustOnly)
                .authentication(AuthenticationFactory.tls(
                        getTlsFileForClient("admin.cert"), getTlsFileForClient("admin.key-pk8")))
                .build();

        assertProduceConsume(client, TOPIC + "-fold", "v5-mtls-via-fold");
    }

    private static void assertProduceConsume(PulsarClient client, String topic, String payload)
            throws Exception {
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();

        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("v5-tls-sub")
                .subscribe();

        producer.newMessage().value(payload).send();

        Message<String> received = consumer.receive(Duration.ofSeconds(10));
        assertThat(received).isNotNull();
        assertThat(received.value()).isEqualTo(payload);
    }
}
