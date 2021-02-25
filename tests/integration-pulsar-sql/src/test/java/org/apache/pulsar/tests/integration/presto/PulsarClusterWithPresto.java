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
package org.apache.pulsar.tests.integration.presto;

import com.google.common.collect.Maps;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.PrestoWorkerContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testcontainers.containers.BindMode;

@Slf4j
public class PulsarClusterWithPresto extends PulsarCluster {
    private PrestoWorkerContainer prestoWorkerContainer;
    @Getter
    private Map<String, PrestoWorkerContainer> sqlFollowWorkerContainers;

    public PulsarClusterWithPresto(PulsarClusterSpec spec) {
        super(spec);
        this.sqlFollowWorkerContainers = Maps.newTreeMap();
        this.prestoWorkerContainer = buildPrestoWorkerContainer(PrestoWorkerContainer.NAME, true, null, null);
    }


    @Override
    public void start() throws Exception {
        super.start();
        prestoWorkerContainer.start();
    }

    public PrestoWorkerContainer getPrestoWorkerContainer() {
        return prestoWorkerContainer;
    }

    public void startPrestoWorker() {
        startPrestoWorker(null, null);
    }

    public void startPrestoWorker(String offloadDriver, String offloadProperties) {
        log.info("[startPrestoWorker] offloadDriver: {}, offloadProperties: {}", offloadDriver, offloadProperties);
        if (null == prestoWorkerContainer) {
            prestoWorkerContainer = buildPrestoWorkerContainer(
                    PrestoWorkerContainer.NAME, true, offloadDriver, offloadProperties);
            prestoWorkerContainer.setDockerImageName(spec.pulsarTestImage());
        }
        prestoWorkerContainer.start();
        log.info("[{}] Presto coordinator start finished.", prestoWorkerContainer.getContainerName());
    }

    public void stopPrestoWorker() {
        if (sqlFollowWorkerContainers != null && sqlFollowWorkerContainers.size() > 0) {
            for (PrestoWorkerContainer followWorker : sqlFollowWorkerContainers.values()) {
                followWorker.stop();
                log.info("Stopped presto follow worker {}.", followWorker.getContainerName());
            }
            sqlFollowWorkerContainers.clear();
            log.info("Stopped all presto follow workers.");
        }
        if (null != prestoWorkerContainer) {
            prestoWorkerContainer.stop();
            log.info("Stopped presto coordinator.");
            prestoWorkerContainer = null;
        }
    }

    public void startPrestoFollowWorkers(int numSqlFollowWorkers, String offloadDriver, String offloadProperties) {
        log.info("start presto follow worker containers.");
        sqlFollowWorkerContainers.putAll(runNumContainers(
                "sql-follow-worker",
                numSqlFollowWorkers,
                (name) -> {
                    log.info("build presto follow worker with name {}", name);
                    return buildPrestoWorkerContainer(name, false, offloadDriver, offloadProperties);
                }
        ));
        // Start workers that have been initialized
        sqlFollowWorkerContainers.values().parallelStream().forEach(PrestoWorkerContainer::start);
        log.info("Successfully started {} presto follow worker containers.", sqlFollowWorkerContainers.size());
    }

    private PrestoWorkerContainer buildPrestoWorkerContainer(String hostName, boolean isCoordinator,
                                                             String offloadDriver, String offloadProperties) {
        String resourcePath = isCoordinator ? "presto-coordinator-config.properties"
                : "presto-follow-worker-config.properties";
        PrestoWorkerContainer container = new PrestoWorkerContainer(
                clusterName, hostName)
                .withNetwork(network)
                .withNetworkAliases(hostName)
                .withEnv("clusterName", clusterName)
                .withEnv("zkServers", ZKContainer.NAME)
                .withEnv("zookeeperServers", ZKContainer.NAME + ":" + ZKContainer.ZK_PORT)
                .withEnv("pulsar.zookeeper-uri", ZKContainer.NAME + ":" + ZKContainer.ZK_PORT)
                .withEnv("pulsar.broker-service-url", "http://pulsar-broker-0:8080")
                .withClasspathResourceMapping(
                        resourcePath, "/pulsar/conf/presto/config.properties", BindMode.READ_WRITE);
        if (spec.queryLastMessage()) {
            container.withEnv("pulsar.bookkeeper-use-v2-protocol", "false")
                    .withEnv("pulsar.bookkeeper-explicit-interval", "10");
        }
        if (offloadDriver != null && offloadProperties != null) {
            log.info("[startPrestoWorker] set offload env offloadDriver: {}, offloadProperties: {}",
                    offloadDriver, offloadProperties);
            // used to query from tiered storage
            container.withEnv("SQL_PREFIX_pulsar.managed-ledger-offload-driver", offloadDriver);
            container.withEnv("SQL_PREFIX_pulsar.offloader-properties", offloadProperties);
            container.withEnv("SQL_PREFIX_pulsar.offloaders-directory", "/pulsar/offloaders");
            container.withEnv("AWS_ACCESS_KEY_ID", "accesskey");
            container.withEnv("AWS_SECRET_KEY", "secretkey");
        }
        log.info("[{}] build presto worker container. isCoordinator: {}, resourcePath: {}",
                container.getContainerName(), isCoordinator, resourcePath);
        container.setDockerImageName(spec.pulsarTestImage());
        return container;
    }


}
