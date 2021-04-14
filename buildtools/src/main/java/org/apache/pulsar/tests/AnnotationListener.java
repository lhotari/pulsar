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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;
import org.testng.internal.annotations.DisabledRetryAnalyzer;

public class AnnotationListener implements IAnnotationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(AnnotationListener.class);
    private static final long DEFAULT_TEST_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static Set<String> EXCLUDED_GROUPS = parseExcludedGroups(
            System.getProperty("testExcludedGroups", ""));

    static Set<String> parseExcludedGroups(String excludedGroupsString) {
        return Collections.unmodifiableSet(new HashSet<>(
                Arrays.asList(StringUtils.split(excludedGroupsString, ", ")
                )));
    }

    private final Set<String> excludedGroups;

    public AnnotationListener() {
        this(EXCLUDED_GROUPS);
    }

    AnnotationListener(Set<String> excludedGroups) {
        this.excludedGroups = excludedGroups;
        LOG.info("Created annotation listener");
    }

    @Override
    public void transform(ITestAnnotation annotation,
                          Class testClass,
                          Constructor testConstructor,
                          Method testMethod) {
        if (annotation.getRetryAnalyzerClass() == null
                || annotation.getRetryAnalyzerClass() == DisabledRetryAnalyzer.class) {
            annotation.setRetryAnalyzer(RetryAnalyzer.class);
        }

        if (annotation.getGroups() != null
                && Arrays.stream(annotation.getGroups()).anyMatch(excludedGroups::contains)) {
            LOG.info("Skipping test method {}", testMethod);
            annotation.setEnabled(false);
        }

        // Enforce default test timeout
        if (annotation.getTimeOut() == 0) {
            annotation.setTimeOut(DEFAULT_TEST_TIMEOUT_MILLIS);
        }
    }
}
