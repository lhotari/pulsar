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
package org.apache.pulsar.tests;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleanup Thread Local state attach to Netty's FastThreadLocal.
 */
public final class FastThreadLocalStateCleaner {
    public static final FastThreadLocalStateCleaner INSTANCE = new FastThreadLocalStateCleaner();
    private static final Logger LOG = LoggerFactory.getLogger(FastThreadLocalStateCleaner.class);
    private static final ThreadLocal<?> SLOW_THREAD_LOCAL_MAP = lookupSlowThreadLocalMap();
    private static final Class<?> FAST_THREAD_LOCAL_CLASS;
    private static final Method SET_THREAD_LOCAL_MAP;
    private static final Method GET_THREAD_LOCAL_MAP;

    static {
        Class<?> clazz = null;
        Method setThreadLocalMapMethod = null;
        Method getThreadLocalMapMethod = null;
        if (SLOW_THREAD_LOCAL_MAP != null) {
            try {
                clazz = ClassUtils.getClass("io.netty.util.concurrent.FastThreadLocalThread");
                Class<?> internalThreadLocalMapClass =
                        ClassUtils.getClass("io.netty.util.internal.InternalThreadLocalMap");
                setThreadLocalMapMethod = MethodUtils
                        .getMatchingAccessibleMethod(clazz, "setThreadLocalMap",
                                internalThreadLocalMapClass);
                getThreadLocalMapMethod = MethodUtils
                        .getMatchingAccessibleMethod(clazz, "threadLocalMap");
            } catch (ClassNotFoundException e) {
                // ignore
                LOG.debug("Ignoring exception", e);
                clazz = null;
                setThreadLocalMapMethod = null;
                getThreadLocalMapMethod = null;
            }
        }
        FAST_THREAD_LOCAL_CLASS = clazz;
        SET_THREAD_LOCAL_MAP = setThreadLocalMapMethod;
        GET_THREAD_LOCAL_MAP = getThreadLocalMapMethod;
    }

    private static ThreadLocal<?> lookupSlowThreadLocalMap() {
        try {
            Field slowThreadLocalMapField = FieldUtils.getDeclaredField(
                    ClassUtils.getClass("io.netty.util.internal.InternalThreadLocalMap"),
                    "slowThreadLocalMap", true);
            if (slowThreadLocalMapField != null) {
                return (ThreadLocal<?>) slowThreadLocalMapField.get(null);
            } else {
                LOG.warn("Cannot find InternalThreadLocalMap.slowThreadLocalMap field."
                        + " This might be due to using an unsupported netty-common version.");
                return null;
            }
        } catch (IllegalAccessException | ClassNotFoundException e) {
            LOG.warn("Cannot find InternalThreadLocalMap.slowThreadLocalMap thread local", e);
            return null;
        }
    }

    // force singleton
    private FastThreadLocalStateCleaner() {

    }

    public <T> void cleanupAllFastThreadLocals(Thread thread, BiConsumer<Thread, T> cleanedValueListener) {
        Objects.nonNull(thread);
        ThreadLocalStateCleaner.INSTANCE.cleanupThreadLocal(SLOW_THREAD_LOCAL_MAP, thread, cleanedValueListener);
        if (FAST_THREAD_LOCAL_CLASS.isInstance(thread)) {
            try {
                Object currentValue = GET_THREAD_LOCAL_MAP.invoke(thread);
                if (currentValue != null) {
                    SET_THREAD_LOCAL_MAP.invoke(thread, new Object[]{null});
                    if (cleanedValueListener != null) {
                        cleanedValueListener.accept(thread, (T) currentValue);
                    }
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                LOG.warn("Cannot reset state for FastLocalThread {}", thread, e);
            }
        }
    }

    // cleanup all fast thread local state on all active threads
    public <T> void cleanupAllFastThreadLocals(BiConsumer<Thread, T> cleanedValueListener) {
        for (Thread thread : ThreadUtils.getAllThreads()) {
            cleanupAllFastThreadLocals(thread, cleanedValueListener);
        }
    }

    public boolean isEnabled() {
        return SLOW_THREAD_LOCAL_MAP != null && FAST_THREAD_LOCAL_CLASS != null;
    }
}
