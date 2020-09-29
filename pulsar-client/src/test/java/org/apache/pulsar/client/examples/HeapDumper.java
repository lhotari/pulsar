/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.examples;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
public class HeapDumper {
    public static HeapDumper INSTANCE = new HeapDumper();

    private Object diagnosticMXBean;
    private Method dumpHeapMethod;

    @SuppressWarnings("unchecked")
    private HeapDumper() {
        try {
            Class<?> diagnosticMXBeanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
            this.diagnosticMXBean = ManagementFactory
                    .getPlatformMXBean((Class<PlatformManagedObject>) diagnosticMXBeanClass);
            this.dumpHeapMethod = diagnosticMXBeanClass.getMethod("dumpHeap", String.class,
                    Boolean.TYPE);
        } catch (Throwable ex) {
            throw new IllegalStateException("Unable to locate HotSpotDiagnosticMXBean", ex);
        }
    }

    private File createTempFile(boolean live) throws IOException {
        String date = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm").format(LocalDateTime.now());
        File file = File.createTempFile("heapdump" + date + (live ? "-live" : ""), ".hprof");
        file.delete();
        return file;
    }

    public void dumpHeap(File file, boolean live) {
        log.info("Creating heap dump...");
        try {
            dumpHeapMethod.invoke(diagnosticMXBean, file.getAbsolutePath(), live);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        log.info("Heap dump created in {}", file);
    }

    public void dumpHeap(boolean live) {
        try {
            dumpHeap(createTempFile(live), live);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public void dumpHeap() {
        dumpHeap(true);
    }
}
