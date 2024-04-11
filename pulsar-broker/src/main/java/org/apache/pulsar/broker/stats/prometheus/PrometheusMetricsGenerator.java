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
package org.apache.pulsar.broker.stats.prometheus;

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils.generateSystemMetrics;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils.getTypeStr;
import static org.apache.pulsar.common.stats.JvmMetrics.getJvmDirectMemoryUsed;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Gauge.Child;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.metrics.ManagedCursorMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerCacheMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.eclipse.jetty.server.HttpOutput;

/**
 * Generate metrics aggregated at the namespace level and optionally at a topic level and formats them out
 * in a text format suitable to be consumed by Prometheus.
 * Format specification can be found at <a
 * href="https://prometheus.io/docs/instrumenting/exposition_formats/">Exposition Formats</a>
 */
@Slf4j
public class PrometheusMetricsGenerator {
    private static final int DEFAULT_INITIAL_BUFFER_SIZE = 1024 * 1024 * 1024; // 1MB
    private static final int MINIMUM_FOR_MAX_COMPONENTS = 64;

    static {
        DefaultExports.initialize();

        Gauge.build("jvm_memory_direct_bytes_used", "-").create().setChild(new Child() {
            @Override
            public double get() {
                return getJvmDirectMemoryUsed();
            }
        }).register(CollectorRegistry.defaultRegistry);

        Gauge.build("jvm_memory_direct_bytes_max", "-").create().setChild(new Child() {
            @Override
            public double get() {
                return DirectMemoryUtils.jvmMaxDirectMemory();
            }
        }).register(CollectorRegistry.defaultRegistry);

        // metric to export pulsar version info
        Gauge.build("pulsar_version_info", "-")
                .labelNames("version", "commit").create()
                .setChild(new Child() {
                    @Override
                    public double get() {
                        return 1.0;
                    }
                }, PulsarVersion.getVersion(), PulsarVersion.getGitSha())
                .register(CollectorRegistry.defaultRegistry);
    }

    private volatile MetricsBuffer metricsBuffer;
    private static AtomicReferenceFieldUpdater<PrometheusMetricsGenerator, MetricsBuffer> metricsBufferFieldUpdater =
            AtomicReferenceFieldUpdater.newUpdater(PrometheusMetricsGenerator.class, MetricsBuffer.class,
                    "metricsBuffer");

    public static class MetricsBuffer {
        private final CompletableFuture<ByteBuf> bufferFuture;
        private final long createTimeslot;
        private final AtomicInteger refCnt = new AtomicInteger(1);

        MetricsBuffer(long timeslot) {
            bufferFuture = new CompletableFuture<>();
            createTimeslot = timeslot;
        }

        public CompletableFuture<ByteBuf> getBufferFuture() {
            return bufferFuture;
        }

        long getCreateTimeslot() {
            return createTimeslot;
        }

        /**
         * Retain the buffer. This is allowed, only when the buffer is not already released.
         *
         * @return true if the buffer is retained successfully, false otherwise.
         */
        boolean retain() {
            return refCnt.updateAndGet(x -> x > 0 ? x + 1 : x) > 0;
        }

        /**
         * Release the buffer.
         */
        public void release() {
            int newValue = refCnt.decrementAndGet();
            if (newValue == 0) {
                if (bufferFuture.isDone() && !bufferFuture.isCompletedExceptionally()) {
                    bufferFuture.getNow(null).release();
                }
            }
        }
    }

    private final PulsarService pulsar;
    private final boolean includeTopicMetrics;
    private final boolean includeConsumerMetrics;
    private final boolean includeProducerMetrics;
    private final boolean splitTopicAndPartitionIndexLabel;

    private volatile int initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;

    public PrometheusMetricsGenerator(PulsarService pulsar, boolean includeTopicMetrics,
                                      boolean includeConsumerMetrics, boolean includeProducerMetrics,
                                      boolean splitTopicAndPartitionIndexLabel) {
        this.pulsar = pulsar;
        this.includeTopicMetrics = includeTopicMetrics;
        this.includeConsumerMetrics = includeConsumerMetrics;
        this.includeProducerMetrics = includeProducerMetrics;
        this.splitTopicAndPartitionIndexLabel = splitTopicAndPartitionIndexLabel;
    }

    private ByteBuf generate0(List<PrometheusRawMetricsProvider> metricsProviders) {
        ByteBuf buf = allocateMultipartCompositeDirectBuffer();
        boolean exceptionHappens = false;
        //Used in namespace/topic and transaction aggregators as share metric names
        PrometheusMetricStreams metricStreams = new PrometheusMetricStreams();
        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);

            generateSystemMetrics(stream, pulsar.getConfiguration().getClusterName());

            NamespaceStatsAggregator.generate(pulsar, includeTopicMetrics, includeConsumerMetrics,
                    includeProducerMetrics, splitTopicAndPartitionIndexLabel, metricStreams);

            if (pulsar.getWorkerServiceOpt().isPresent()) {
                pulsar.getWorkerService().generateFunctionsStats(stream);
            }

            if (pulsar.getConfiguration().isTransactionCoordinatorEnabled()) {
                TransactionAggregator.generate(pulsar, metricStreams, includeTopicMetrics);
            }

            metricStreams.flushAllToStream(stream);

            generateBrokerBasicMetrics(pulsar, stream);

            generateManagedLedgerBookieClientMetrics(pulsar, stream);

            if (metricsProviders != null) {
                for (PrometheusRawMetricsProvider metricsProvider : metricsProviders) {
                    metricsProvider.generate(stream);
                }
            }

            return buf;
        } catch (Throwable t) {
            exceptionHappens = true;
            throw t;
        } finally {
            //release all the metrics buffers
            metricStreams.releaseAll();
            //if exception happens, release buffer
            if (exceptionHappens) {
                buf.release();
            } else {
                // for the next time, the initial buffer size will be suggested by the last buffer size
                initialBufferSize = Math.max(DEFAULT_INITIAL_BUFFER_SIZE, buf.readableBytes());
            }
        }
    }

    private ByteBuf allocateMultipartCompositeDirectBuffer() {
        // use composite buffer with pre-allocated buffers to ensure that the pooled allocator can be used
        // for allocating the buffers
        ByteBufAllocator byteBufAllocator = PulsarByteBufAllocator.DEFAULT;
        long chunkSize;
        if (byteBufAllocator instanceof PooledByteBufAllocator) {
            PooledByteBufAllocator pooledByteBufAllocator = (PooledByteBufAllocator) byteBufAllocator;
            chunkSize = Math.max(pooledByteBufAllocator.metric().chunkSize(), DEFAULT_INITIAL_BUFFER_SIZE);
        } else {
            chunkSize = DEFAULT_INITIAL_BUFFER_SIZE;
        }
        CompositeByteBuf buf = byteBufAllocator.compositeDirectBuffer(
                Math.max(MINIMUM_FOR_MAX_COMPONENTS, (int) (initialBufferSize / chunkSize) + 1));
        int totalLen = 0;
        while (totalLen < initialBufferSize) {
            totalLen += chunkSize;
            buf.addComponent(false, byteBufAllocator.directBuffer((int) chunkSize));
        }
        return buf;
    }

    private static void generateBrokerBasicMetrics(PulsarService pulsar, SimpleTextOutputStream stream) {
        String clusterName = pulsar.getConfiguration().getClusterName();
        // generate managedLedgerCache metrics
        parseMetricsToPrometheusMetrics(new ManagedLedgerCacheMetrics(pulsar).generate(),
                clusterName, Collector.Type.GAUGE, stream);

        if (pulsar.getConfiguration().isExposeManagedLedgerMetricsInPrometheus()) {
            // generate managedLedger metrics
            parseMetricsToPrometheusMetrics(new ManagedLedgerMetrics(pulsar).generate(),
                    clusterName, Collector.Type.GAUGE, stream);
        }

        if (pulsar.getConfiguration().isExposeManagedCursorMetricsInPrometheus()) {
            // generate managedCursor metrics
            parseMetricsToPrometheusMetrics(new ManagedCursorMetrics(pulsar).generate(),
                    clusterName, Collector.Type.GAUGE, stream);
        }

        parseMetricsToPrometheusMetrics(pulsar.getBrokerService()
                        .getPulsarStats().getBrokerOperabilityMetrics().getMetrics(),
                clusterName, Collector.Type.GAUGE, stream);

        // generate loadBalance metrics
        parseMetricsToPrometheusMetrics(pulsar.getLoadManager().get().getLoadBalancingMetrics(),
                clusterName, Collector.Type.GAUGE, stream);
    }

    private static void parseMetricsToPrometheusMetrics(Collection<Metrics> metrics, String cluster,
                                                        Collector.Type metricType, SimpleTextOutputStream stream) {
        Set<String> names = new HashSet<>();
        for (Metrics metrics1 : metrics) {
            for (Map.Entry<String, Object> entry : metrics1.getMetrics().entrySet()) {
                String value = null;
                if (entry.getKey().contains(".")) {
                    try {
                        String key = entry.getKey();
                        int dotIndex = key.indexOf(".");
                        int nameIndex = key.substring(0, dotIndex).lastIndexOf("_");
                        if (nameIndex == -1) {
                            continue;
                        }

                        String name = key.substring(0, nameIndex);
                        value = key.substring(nameIndex + 1);
                        if (!names.contains(name)) {
                            stream.write("# TYPE ").write(name.replace("brk_", "pulsar_")).write(' ')
                                    .write(getTypeStr(metricType)).write("\n");
                            names.add(name);
                        }
                        stream.write(name.replace("brk_", "pulsar_"))
                                .write("{cluster=\"").write(cluster).write('"');
                    } catch (Exception e) {
                        continue;
                    }
                } else {


                    String name = entry.getKey();
                    if (!names.contains(name)) {
                        stream.write("# TYPE ").write(entry.getKey().replace("brk_", "pulsar_")).write(' ')
                                .write(getTypeStr(metricType)).write('\n');
                        names.add(name);
                    }
                    stream.write(name.replace("brk_", "pulsar_"))
                            .write("{cluster=\"").write(cluster).write('"');
                }

                //to avoid quantile label duplicated
                boolean appendedQuantile = false;
                for (Map.Entry<String, String> metric : metrics1.getDimensions().entrySet()) {
                    if (metric.getKey().isEmpty() || "cluster".equals(metric.getKey())) {
                        continue;
                    }
                    stream.write(", ").write(metric.getKey()).write("=\"").write(metric.getValue()).write('"');
                    if (value != null && !value.isEmpty() && !appendedQuantile) {
                        stream.write(", ").write("quantile=\"").write(value).write('"');
                        appendedQuantile = true;
                    }
                }
                stream.write("} ").write(String.valueOf(entry.getValue())).write("\n");
            }
        }
    }

    private static void generateManagedLedgerBookieClientMetrics(PulsarService pulsar, SimpleTextOutputStream stream) {
        StatsProvider statsProvider = pulsar.getManagedLedgerClientFactory().getStatsProvider();
        if (statsProvider instanceof NullStatsProvider) {
            return;
        }

        try {
            Writer writer = new StringWriter();
            statsProvider.writeAllMetrics(writer);
            stream.write(writer.toString());
        } catch (IOException e) {
            // nop
        }
    }

    public void generate(OutputStream out,
                                      List<PrometheusRawMetricsProvider> metricsProviders) throws IOException {
        MetricsBuffer metricsBuffer = renderToBuffer(metricsProviders);
        try {
            ByteBuf buffer = null;
            try {
                buffer = metricsBuffer.getBufferFuture()
                        .get(pulsar.getConfiguration().getMetricsServletTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (ExecutionException | TimeoutException e) {
                throw new IOException(e);
            }
            if (buffer == null) {
                return;
            }
            if (out instanceof HttpOutput) {
                HttpOutput output = (HttpOutput) out;
                //no mem_copy and memory allocations here
                ByteBuffer[] buffers = buffer.nioBuffers();
                for (ByteBuffer buffer0 : buffers) {
                    output.write(buffer0);
                }
            } else {
                buffer.readBytes(out, buffer.readableBytes());
            }
        } finally {
            metricsBuffer.release();
        }
    }

    public MetricsBuffer renderToBuffer(List<PrometheusRawMetricsProvider> metricsProviders) {
        boolean cacheMetricsResponse = pulsar.getConfiguration().isMetricsBufferResponse();
        long currentTimeSlot = cacheMetricsResponse ? calculateCurrentTimeSlot() : 0;
        MetricsBuffer currentMetricsBuffer = metricsBuffer;
        if (currentMetricsBuffer == null || !currentMetricsBuffer.retain()
                || (currentMetricsBuffer.getBufferFuture().isDone()
                && (currentMetricsBuffer.getCreateTimeslot() == 0
                || currentTimeSlot > currentMetricsBuffer.getCreateTimeslot()))) {
            MetricsBuffer newMetricsBuffer = new MetricsBuffer(currentTimeSlot);
            if (metricsBufferFieldUpdater.compareAndSet(this, currentMetricsBuffer, newMetricsBuffer)) {
                if (currentMetricsBuffer != null) {
                    currentMetricsBuffer.release();
                }
                CompletableFuture<ByteBuf> bufferFuture = newMetricsBuffer.getBufferFuture();
                try {
                    bufferFuture.complete(generate0(metricsProviders));
                } catch (Exception e) {
                    bufferFuture.completeExceptionally(e);
                }
                currentMetricsBuffer = newMetricsBuffer;
            } else {
                currentMetricsBuffer = metricsBuffer;
            }
        }
        return currentMetricsBuffer;
    }

    /**
     * Calculate the current time slot based on the current time.
     * This is to ensure that cached metrics are refreshed consistently at a fixed interval regardless of the request
     * time.
     */
    private long calculateCurrentTimeSlot() {
        long cacheTimeoutMillis =
                TimeUnit.SECONDS.toMillis(Math.max(1, pulsar.getConfiguration().getManagedLedgerStatsPeriodSeconds()));
        long now = System.currentTimeMillis();
        return now / cacheTimeoutMillis;
    }
}
