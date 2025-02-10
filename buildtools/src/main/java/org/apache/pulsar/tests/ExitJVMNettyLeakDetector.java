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
package org.apache.pulsar.tests;

import io.netty.util.ResourceLeakDetector;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom Netty leak detector that dumps the leak to a file and exits the JVM.
 */
public class ExitJVMNettyLeakDetector<T> extends ResourceLeakDetector<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ExitJVMNettyLeakDetector.class);
    private static final File DUMP_DIR =
            new File(System.getenv().getOrDefault("NETTY_LEAK_DUMP_DIR", System.getProperty("java.io.tmpdir")));

    public ExitJVMNettyLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        super(resourceType, samplingInterval, maxActive);
    }

    public ExitJVMNettyLeakDetector(Class<?> resourceType, int samplingInterval) {
        super(resourceType, samplingInterval);
    }

    public ExitJVMNettyLeakDetector(String resourceType, int samplingInterval, long maxActive) {
        super(resourceType, samplingInterval, maxActive);
    }


    @Override
    protected void reportTracedLeak(String resourceType, String records) {
        super.reportTracedLeak(resourceType, records);
        dumpToFile(resourceType, records);
        exitJVM();
    }

    @Override
    protected void reportUntracedLeak(String resourceType) {
        super.reportUntracedLeak(resourceType);
        dumpToFile(resourceType, null);
        exitJVM();
    }

    private void exitJVM() {
        Runtime.getRuntime().halt(1);
    }

    private void dumpToFile(String resourceType, String records) {
        try {
            if (!DUMP_DIR.exists()) {
                DUMP_DIR.mkdirs();
            }
            String datetimePart =
                    DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS").format(ZonedDateTime.now());
            File nettyLeakDumpFile =
                    new File(DUMP_DIR, "netty_leak_" + datetimePart + ".txt");
            try (PrintWriter out = new PrintWriter(nettyLeakDumpFile)) {
                if (records != null) {
                    out.println("Traced leak detected " + resourceType);
                    out.println(records);
                } else {
                    out.println("Untraced leak detected " + resourceType);
                }
                out.flush();
            }
        } catch (IOException e) {
            LOG.error("Cannot write thread leak dump", e);
        }
    }
}
