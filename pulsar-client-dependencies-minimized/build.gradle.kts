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

plugins {
    id("pulsar.shadow-conventions")
}

dependencies {
    implementation(project(":pulsar-client-original"))
}

tasks.shadowJar {
    dependencies {
        include(dependency("it.unimi.dsi:fastutil"))
    }
    // Shadow's minimize() is too aggressive (keeps ~12,885 classes vs the ~590 that are needed).
    // Instead, we whitelist the specific classes used by pulsar-client and pulsar-broker.
    // The patterns below match Maven shade plugin's minimizeJar output, plus broker-only classes.
    include("META-INF/**")

    // Root package utilities (Hash, HashCommon, Arrays, etc.)
    include("it/unimi/dsi/fastutil/*.class")

    // --- longs package ---
    // Long2Object maps (used by NegativeAcksTracker, InMemoryRedeliveryTracker, PendingAcksMap, etc.)
    include("it/unimi/dsi/fastutil/longs/Long2Object*")
    include("it/unimi/dsi/fastutil/longs/AbstractLong2Object*")
    // Long2Int maps (used by InMemoryRedeliveryTracker)
    include("it/unimi/dsi/fastutil/longs/Long2Int*")
    include("it/unimi/dsi/fastutil/longs/AbstractLong2Int*")
    // Cross-type function interfaces (use exact names to avoid matching Functions$* inner classes)
    for (t in listOf("Boolean", "Byte", "Char", "Double", "Float", "Int", "Long", "Object", "Reference", "Short")) {
        include("it/unimi/dsi/fastutil/longs/Long2${t}Function.class")
    }
    // LongOpenHashSet (used by InMemoryDelayedDeliveryTracker)
    include("it/unimi/dsi/fastutil/longs/LongOpenHashSet*")
    // Collection/iterator/utility support classes
    include("it/unimi/dsi/fastutil/longs/AbstractLongCollection.class")
    include("it/unimi/dsi/fastutil/longs/AbstractLongIterator.class")
    include("it/unimi/dsi/fastutil/longs/AbstractLongList*")
    include("it/unimi/dsi/fastutil/longs/AbstractLongSet.class")
    include("it/unimi/dsi/fastutil/longs/AbstractLongSortedSet.class")
    include("it/unimi/dsi/fastutil/longs/AbstractLongSpliterator.class")
    include("it/unimi/dsi/fastutil/longs/LongArrayList*")
    include("it/unimi/dsi/fastutil/longs/LongArraySet*")
    include("it/unimi/dsi/fastutil/longs/LongArrays*")
    include("it/unimi/dsi/fastutil/longs/LongBidirectional*")
    include("it/unimi/dsi/fastutil/longs/LongBigArrays*")
    include("it/unimi/dsi/fastutil/longs/LongBigListIterator*")
    include("it/unimi/dsi/fastutil/longs/LongCollection*")
    include("it/unimi/dsi/fastutil/longs/LongComparator*")
    include("it/unimi/dsi/fastutil/longs/LongConsumer*")
    include("it/unimi/dsi/fastutil/longs/LongImmutableList*")
    include("it/unimi/dsi/fastutil/longs/LongIterable*")
    include("it/unimi/dsi/fastutil/longs/LongIterator*")
    include("it/unimi/dsi/fastutil/longs/LongList*")
    include("it/unimi/dsi/fastutil/longs/LongObjectImmutablePair*")
    include("it/unimi/dsi/fastutil/longs/LongObjectPair*")
    include("it/unimi/dsi/fastutil/longs/LongPredicate*")
    include("it/unimi/dsi/fastutil/longs/LongSet*")
    include("it/unimi/dsi/fastutil/longs/LongSorted*")
    include("it/unimi/dsi/fastutil/longs/LongSpliterator*")
    include("it/unimi/dsi/fastutil/longs/LongStack*")
    include("it/unimi/dsi/fastutil/longs/LongUnaryOperator*")
    include("it/unimi/dsi/fastutil/longs/package-info.class")

    // --- objects package ---
    // ObjectBidirectionalIterator (used by PendingAcksMap)
    include("it/unimi/dsi/fastutil/objects/ObjectBidirectionalIterable*")
    include("it/unimi/dsi/fastutil/objects/ObjectBidirectionalIterator*")
    // ObjectIntPair (used by Consumer)
    include("it/unimi/dsi/fastutil/objects/ObjectIntPair.class")
    include("it/unimi/dsi/fastutil/objects/ObjectObjectImmutablePair*")
    // Cross-type function interfaces (use exact names to avoid matching Functions$*Function inner classes)
    @Suppress("SpellCheckingInspection")
    val objectFunctionTypes = listOf(
        "Boolean", "Byte", "Char", "Double", "Float", "Int", "Long", "Object", "Reference", "Short",
    )
    for (t in objectFunctionTypes) {
        include("it/unimi/dsi/fastutil/objects/Object2${t}Function.class")
        include("it/unimi/dsi/fastutil/objects/Reference2${t}Function.class")
    }
    // Collection/iterator/utility support classes
    include("it/unimi/dsi/fastutil/objects/AbstractObjectCollection.class")
    include("it/unimi/dsi/fastutil/objects/AbstractObjectIterator.class")
    include("it/unimi/dsi/fastutil/objects/AbstractObjectList*")
    include("it/unimi/dsi/fastutil/objects/AbstractObjectSet.class")
    include("it/unimi/dsi/fastutil/objects/AbstractObjectSortedSet.class")
    include("it/unimi/dsi/fastutil/objects/AbstractObjectSpliterator.class")
    include("it/unimi/dsi/fastutil/objects/ObjectArrayList*")
    include("it/unimi/dsi/fastutil/objects/ObjectArraySet*")
    include("it/unimi/dsi/fastutil/objects/ObjectArrays*")
    include("it/unimi/dsi/fastutil/objects/ObjectBigArrays*")
    include("it/unimi/dsi/fastutil/objects/ObjectBigListIterator.class")
    include("it/unimi/dsi/fastutil/objects/ObjectCollection.class")
    include("it/unimi/dsi/fastutil/objects/ObjectCollections*")
    include("it/unimi/dsi/fastutil/objects/ObjectComparators*")
    include("it/unimi/dsi/fastutil/objects/ObjectImmutableList*")
    include("it/unimi/dsi/fastutil/objects/ObjectIterable.class")
    include("it/unimi/dsi/fastutil/objects/ObjectIterator.class")
    include("it/unimi/dsi/fastutil/objects/ObjectIterators*")
    include("it/unimi/dsi/fastutil/objects/ObjectList.class")
    include("it/unimi/dsi/fastutil/objects/ObjectListIterator.class")
    include("it/unimi/dsi/fastutil/objects/ObjectLists*")
    include("it/unimi/dsi/fastutil/objects/ObjectOpenHashSet*")
    include("it/unimi/dsi/fastutil/objects/ObjectSet.class")
    include("it/unimi/dsi/fastutil/objects/ObjectSets*")
    include("it/unimi/dsi/fastutil/objects/ObjectSortedSet.class")
    include("it/unimi/dsi/fastutil/objects/ObjectSortedSets*")
    include("it/unimi/dsi/fastutil/objects/ObjectSpliterator.class")
    include("it/unimi/dsi/fastutil/objects/ObjectSpliterators*")
    include("it/unimi/dsi/fastutil/objects/package-info.class")

    // --- ints package (used by broker: DrainingHashesTracker, Consumer, PersistentStickyKeyDispatcher) ---
    include("it/unimi/dsi/fastutil/ints/Int2Object*")
    include("it/unimi/dsi/fastutil/ints/AbstractInt2Object*")
    for (t in listOf("Boolean", "Byte", "Char", "Double", "Float", "Int", "Long", "Object", "Reference", "Short")) {
        include("it/unimi/dsi/fastutil/ints/Int2${t}Function.class")
    }
    include("it/unimi/dsi/fastutil/ints/IntOpenHashSet*")
    include("it/unimi/dsi/fastutil/ints/IntIntPair*")
    include("it/unimi/dsi/fastutil/ints/IntIntImmutablePair*")
    include("it/unimi/dsi/fastutil/ints/IntSet*")
    include("it/unimi/dsi/fastutil/ints/AbstractIntCollection*")
    include("it/unimi/dsi/fastutil/ints/AbstractIntIterator*")
    include("it/unimi/dsi/fastutil/ints/AbstractIntSet*")
    include("it/unimi/dsi/fastutil/ints/AbstractIntSpliterator*")
    include("it/unimi/dsi/fastutil/ints/IntCollection*")
    include("it/unimi/dsi/fastutil/ints/IntIterator*")
    include("it/unimi/dsi/fastutil/ints/IntIterable*")
    include("it/unimi/dsi/fastutil/ints/IntConsumer*")
    include("it/unimi/dsi/fastutil/ints/IntPredicate*")
    include("it/unimi/dsi/fastutil/ints/IntSpliterator*")
    include("it/unimi/dsi/fastutil/ints/IntArrays*")
    include("it/unimi/dsi/fastutil/ints/IntBigArrays*")
    include("it/unimi/dsi/fastutil/ints/IntComparator*")
    include("it/unimi/dsi/fastutil/ints/package-info.class")

    // --- io package (transitively referenced) ---
    include("it/unimi/dsi/fastutil/io/**")

    // --- other primitive types: only cross-type Function interfaces + basic utilities ---
    // Use type-prefixed patterns to avoid matching inner classes (e.g., *Iterator.class would
    // match AbstractByte2BooleanSortedMap$KeySetIterator.class).
    @Suppress("SpellCheckingInspection")
    val otherTypes = mapOf(
        "booleans" to "Boolean", "bytes" to "Byte", "chars" to "Char",
        "doubles" to "Double", "floats" to "Float", "shorts" to "Short",
    )
    for ((pkg, prefix) in otherTypes) {
        for (t in listOf("Boolean", "Byte", "Char", "Double", "Float", "Int", "Long", "Object", "Reference", "Short")) {
            include("it/unimi/dsi/fastutil/$pkg/${prefix}2${t}Function.class")
        }
        include("it/unimi/dsi/fastutil/$pkg/${prefix}Arrays*")
        include("it/unimi/dsi/fastutil/$pkg/${prefix}BigArrays*")
        include("it/unimi/dsi/fastutil/$pkg/${prefix}Comparator.class")
        include("it/unimi/dsi/fastutil/$pkg/${prefix}Comparators*")
        include("it/unimi/dsi/fastutil/$pkg/${prefix}Consumer.class")
        include("it/unimi/dsi/fastutil/$pkg/${prefix}Iterator.class")
        include("it/unimi/dsi/fastutil/$pkg/${prefix}Spliterator.class")
        include("it/unimi/dsi/fastutil/$pkg/package-info.class")
    }
}

