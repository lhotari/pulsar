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
package org.apache.pulsar.tests.performance.launcher;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.apache.pulsar.tests.performance.tools.IotScenario;

/**
 * Unloads a random fraction of the workload's topics every interval while the workload runs, one topic at a time, as
 * a rolling restart of the brokers moves them, and records each unload's time in
 * {@link RunReport#TOPIC_UNLOADS_FILE}: the time the broker took to close the topic, which includes disconnecting
 * its producers and consumers. A failed unload is recorded with its error and doesn't fail the run.
 */
final class TopicUnloads implements AutoCloseable {
    // The longest that one unload may take
    private static final int REQUEST_TIMEOUT_SECONDS = 120;

    /** The admin operation of the unloads, which a test fakes. */
    interface TopicOperations {
        void unload(String topic) throws Exception;
    }

    private final TopicOperations topicOperations;
    private final AutoCloseable resources;
    private final List<String> topics;
    private final IotScenario.TopicUnloads settings;
    private final BufferedWriter csv;
    private final Consumer<String> status;
    private final Thread thread;
    private volatile boolean stopping;

    private TopicUnloads(TopicOperations topicOperations, AutoCloseable resources, List<String> topics,
                         IotScenario.TopicUnloads settings, BufferedWriter csv, Consumer<String> status) {
        this.topicOperations = topicOperations;
        this.resources = resources;
        this.topics = topics;
        this.settings = settings;
        this.csv = csv;
        this.status = status;
        this.thread = new Thread(this::run, "topic-unloads");
        thread.setDaemon(true);
    }

    static TopicUnloads start(String adminUrl, List<String> topics, IotScenario.TopicUnloads settings,
                              Path runOutput, Consumer<String> status) throws IOException {
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl)
                .connectionTimeout(10, TimeUnit.SECONDS)
                .readTimeout(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .requestTimeout(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .build();
        try {
            return start(topic -> admin.topics().unload(topic), admin, topics, settings, runOutput, status);
        } catch (IOException | RuntimeException e) {
            admin.close();
            throw e;
        }
    }

    static TopicUnloads start(TopicOperations topicOperations, AutoCloseable resources, List<String> topics,
                              IotScenario.TopicUnloads settings, Path runOutput, Consumer<String> status)
            throws IOException {
        BufferedWriter csv = Files.newBufferedWriter(runOutput.resolve(RunReport.TOPIC_UNLOADS_FILE));
        csv.write(RunReport.TOPIC_UNLOADS_HEADER);
        csv.newLine();
        csv.flush();
        TopicUnloads unloads = new TopicUnloads(topicOperations, resources, topics, settings, csv, status);
        status.accept(String.format(Locale.ROOT, "Unloading %d of the %d topic(s) every %d s, one at a time",
                unloadsPerRound(topics.size(), settings), topics.size(), settings.intervalSeconds()));
        unloads.thread.start();
        return unloads;
    }

    static int unloadsPerRound(int topicCount, IotScenario.TopicUnloads settings) {
        return Math.min(topicCount, Math.max(1, (int) Math.ceil(topicCount * settings.fraction())));
    }

    private void run() {
        int perRound = unloadsPerRound(topics.size(), settings);
        try {
            while (!stopping) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(settings.intervalSeconds()));
                List<String> shuffled = new ArrayList<>(topics);
                Collections.shuffle(shuffled);
                for (String topic : shuffled.subList(0, perRound)) {
                    if (stopping) {
                        return;
                    }
                    unload(topic);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void unload(String topic) {
        long epochMs = System.currentTimeMillis();
        long start = System.nanoTime();
        String error = "";
        try {
            topicOperations.unload(topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error = "interrupted";
        } catch (Exception e) {
            error = String.valueOf(e.getMessage()).replaceAll("[,\\r\\n]", " ");
        }
        long micros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
        try {
            csv.write(epochMs + "," + topic + "," + micros + "," + error);
            csv.newLine();
            csv.flush();
        } catch (IOException e) {
            status.accept("Topic unloads: couldn't record an unload: " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        stopping = true;
        thread.interrupt();
        thread.join(TimeUnit.SECONDS.toMillis(REQUEST_TIMEOUT_SECONDS));
        try {
            csv.close();
        } finally {
            resources.close();
        }
    }
}
