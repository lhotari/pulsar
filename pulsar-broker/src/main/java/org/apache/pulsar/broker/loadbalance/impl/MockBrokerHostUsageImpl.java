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
package org.apache.pulsar.broker.loadbalance.impl;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

@Slf4j
public class MockBrokerHostUsageImpl implements BrokerHostUsage {
    private final File resourceUsageFile;
    SystemResourceUsage systemResourceUsage;
    long lastModified = -1;

    public MockBrokerHostUsageImpl(File resourceUsageFile, ScheduledExecutorService executorService) {
        this.resourceUsageFile = resourceUsageFile;
        calculateBrokerHostUsage();
        executorService.scheduleAtFixedRate(catchingAndLoggingThrowables(this::calculateBrokerHostUsage),
                5,
                5, TimeUnit.SECONDS);
    }

    @Override
    public SystemResourceUsage getBrokerHostUsage() {
        return systemResourceUsage;
    }

    @Override
    public void calculateBrokerHostUsage() {
        if (!resourceUsageFile.exists()) {
            resourceUsageFile.getParentFile().mkdirs();
            systemResourceUsage = new SystemResourceUsage();
            systemResourceUsage.setCpu(generateCpuResourceUsage());
            try {
                ObjectMapperFactory.getThreadLocal().writeValue(resourceUsageFile, systemResourceUsage);
            } catch (IOException e) {
                log.error("Failed to write to " + resourceUsageFile.getAbsolutePath(), e);
                return;
            }
            lastModified = resourceUsageFile.lastModified();
        } else if (lastModified != resourceUsageFile.lastModified()) {
            try {
                systemResourceUsage =
                        ObjectMapperFactory.getThreadLocal().readValue(resourceUsageFile, SystemResourceUsage.class);
            } catch (IOException e) {
                log.error("Failed to read resource usage from file " + resourceUsageFile.getAbsolutePath(), e);
            }
            lastModified = resourceUsageFile.lastModified();
        }
    }

    private ResourceUsage generateCpuResourceUsage() {
        ResourceUsage cpu = new ResourceUsage();
        cpu.limit = 100;
        int podIndex = resolvePodIndexFromHostName();
        if (podIndex >= 0) {
            // this will reproduce a sequence of 96, 71, 46, 21
            // based on the pod index so that the result is consistent
            cpu.usage = 96 - 25 * (podIndex % 4);
        } else {
            // use random usage
            cpu.usage = ThreadLocalRandom.current().nextInt(100);
        }
        return cpu;
    }

    private int resolvePodIndexFromHostName() {
        Pattern lastDigits = Pattern.compile("^.*-(\\d+)$");
        String hostname = System.getenv("HOSTNAME");
        if (hostname == null) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                log.warn("Cannot resolve hostname", e);
                // ignore
                return -1;
            }
        }
        if (hostname != null) {
            Matcher matcher = lastDigits.matcher(hostname);
            if (matcher.matches()) {
                return Integer.parseInt(matcher.group(1));
            }
        }
        return -1;
    }
}
