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
package org.apache.pulsar.policies.data.loadbalancer;

import lombok.EqualsAndHashCode;

/**
 * POJO used to represents any system specific resource usage this is the format that load manager expects it in.
 */
@EqualsAndHashCode
public class ResourceUsage {
    public final double usage;
    public final double limit;
    private final float percentUsage;

    public ResourceUsage(double usage, double limit) {
        this.usage = usage;
        this.limit = limit;
        this.percentUsage = calculatePercentUsage();
    }

    public ResourceUsage(ResourceUsage that) {
        this.usage = that.usage;
        this.limit = that.limit;
        this.percentUsage = calculatePercentUsage();
    }

    public ResourceUsage() {
        this.usage = -1;
        this.limit = -1;
        this.percentUsage = calculatePercentUsage();
    }

    private float calculatePercentUsage() {
        if (limit <= 0) {
            return 0;
        }
        return (float) ((usage / limit) * 100.0d);
    }

    public int compareTo(ResourceUsage o) {
        return Float.compare(percentUsage(), o.percentUsage);
    }

    public float percentUsage() {
        return percentUsage;
    }
}
