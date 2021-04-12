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
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.DataProvider;

/**
 * TestNG DataProvider for passing all Enum values as parameters to a test method or a test constructor.
 * <p>
 * Example of passing Enum values to a test method:
 * <p>
 * {@code @Test(dataProviderClass = EnumValuesDataProvider.class, dataProvider = "values")}
 * <pre> public void testMethod(SomeEnumClass enumvalue) {</pre>
 * <p>
 * Example of passing Enum values to a test constructor:
 * <p>
 * {@code @Factory(dataProviderClass = EnumValuesDataProvider.class, dataProvider = "values")}
 * <pre> public SomeTestClass(SomeEnumClass enumvalue) {</pre>
 * <p>
 * Supports currently a single Enum parameter for a test method or a test constructor.
 */
public abstract class EnumValuesDataProvider {
    @DataProvider
    public static final Object[][] values(Method testMethod, Constructor<?> testConstructor) {
        Executable testMethodOrConstructor = Objects.requireNonNull(testMethod != null ? testMethod :
                        testConstructor != null ? testConstructor : null,
                "testMethod or testConstructor must be non-null");
        Class<?> enumClass = Arrays.stream(
                testMethodOrConstructor.getParameterTypes())
                .findFirst()
                .filter(Class::isEnum)
                .orElseThrow(() -> new IllegalArgumentException("The test method should have an enum parameter."));
        return toDataProviderArray((Class<? extends Enum<?>>) enumClass);
    }

    /*
     * Converts all values of an Enum class to a TestNG DataProvider object array
     */
    public static Object[][] toDataProviderArray(Class<? extends Enum<?>> enumClass) {
        Enum<?>[] enumValues = enumClass.getEnumConstants();
        return Stream.of(enumValues)
                .map(enumValue -> new Object[]{enumValue})
                .collect(Collectors.toList())
                .toArray(new Object[0][]);
    }
}
