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
package org.apache.pulsar.broker.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Pure deadline-anchored planning. All times are elapsed nanoseconds from one immutable shutdown start.
 * The shared fluid forecast is approximate; physical admission and completion remain the controller's job.
 */
final class ShutdownDrainPlanner {
    record Job(String id, double impact, long topics, long longestTopicNanos, long minimumDurationNanos, boolean idle) {
        Job {
            if (id == null || !Double.isFinite(impact) || impact < 0 || (!idle && impact == 0)
                    || topics < 0 || longestTopicNanos < 0 || minimumDurationNanos < 0) {
                throw new IllegalArgumentException("Invalid shutdown job");
            }
        }
    }

    /** Remaining predictions for actual occupied topic slots; Long.MAX_VALUE means unavailable capacity. */
    record Active(String id, List<Long> topicRemainingNanos, long queuedTopics, long minimumRemainingNanos) {
        Active {
            topicRemainingNanos = List.copyOf(topicRemainingNanos);
            if (id == null || queuedTopics < 0 || minimumRemainingNanos < 0
                    || topicRemainingNanos.stream().anyMatch(remaining -> remaining < 0)) {
                throw new IllegalArgumentException("Invalid active shutdown work");
            }
        }
    }

    enum Reason { CAPACITY, RATE_CAP }

    record Scheduled(Job job, long nominalNanos, long latestNanos, long dueNanos, long notBeforeNanos) { }

    record Plan(List<Scheduled> jobs, boolean workConserving, boolean newExhaustion,
                Set<Reason> reasons, long shortfallLowerBoundNanos) { }

    private final long budget;
    private final long workCutoff;
    private final int topicCapacity;
    private final int bundleCapacity;
    private final long startInterval;
    private final Map<String, Double> started = new HashMap<>();
    private final Map<String, Long> previousDue = new HashMap<>();
    private final Set<Reason> exhaustion = EnumSet.noneOf(Reason.class);
    private long lastStart = -1;
    private long lastPlan;

    ShutdownDrainPlanner(long budgetNanos, long completionTailNanos, int topicCapacity, int bundleCapacity,
                         long startIntervalNanos) {
        if (budgetNanos < 0 || completionTailNanos < 0 || topicCapacity < 1 || bundleCapacity < 1
                || startIntervalNanos < 0) {
            throw new IllegalArgumentException("Invalid shutdown planning limits");
        }
        budget = budgetNanos;
        workCutoff = Math.max(0, budgetNanos - Math.min(budgetNanos, completionTailNanos));
        this.topicCapacity = topicCapacity;
        this.bundleCapacity = bundleCapacity;
        startInterval = startIntervalNanos;
    }

    void started(Job job, long elapsedNanos) {
        if (elapsedNanos < 0 || started.putIfAbsent(job.id(), job.idle() ? 0 : job.impact()) != null) {
            throw new IllegalArgumentException("Duplicate or negative shutdown start");
        }
        lastStart = Math.max(lastStart, elapsedNanos);
        previousDue.remove(job.id());
    }

    Plan plan(long now, Collection<Job> pending, List<Active> active, List<Long> backgroundTopicRemainingNanos,
              ShutdownCloseTimeEstimator.Estimate estimate) {
        if (now < lastPlan) {
            throw new IllegalArgumentException("Shutdown planning time moved backwards");
        }
        lastPlan = now;
        List<Job> ordered = new ArrayList<>(pending);
        Set<String> identities = new HashSet<>();
        for (Job job : ordered) {
            if (!identities.add(job.id()) || started.containsKey(job.id())) {
                throw new IllegalArgumentException("Duplicate or already-started pending job: " + job.id());
            }
        }
        Set<String> runningIdentities = new HashSet<>();
        for (Active running : active) {
            if (!runningIdentities.add(running.id()) || identities.contains(running.id())) {
                throw new IllegalArgumentException("Duplicate active shutdown job: " + running.id());
            }
        }
        ordered.sort(Comparator.comparingDouble((Job job) -> job.idle() ? 0 : job.impact()).reversed()
                .thenComparing(Comparator.comparingLong(Job::topics).reversed()).thenComparing(Job::id));
        boolean previouslyExhausted = !exhaustion.isEmpty();
        boolean unbounded = budget == Long.MAX_VALUE;
        Map<String, Long> latest = new HashMap<>();
        long shortfall = 0;
        if (!unbounded) {
            // A percentile overrun is duration evidence, not deadline exhaustion. Only the
            // shared capacity forecast below decides whether pacing can still fit the budget.
            List<Long> occupied = new ArrayList<>(backgroundTopicRemainingNanos);
            active.forEach(job -> occupied.addAll(job.topicRemainingNanos()));
            CapacityTimeline capacity = new CapacityTimeline(now, workCutoff, topicCapacity, occupied);
            List<Long> availableBundles = new ArrayList<>();
            for (Active job : active) {
                Allocation queued = capacity.forward(job.queuedTopics() * (double) estimate.nanos());
                long finish = add(now, job.minimumRemainingNanos());
                for (long remaining : job.topicRemainingNanos()) {
                    finish = Math.max(finish, add(now, remaining));
                }
                if (job.queuedTopics() > 0) {
                    finish = Math.max(finish, Math.max(queued.finish(), add(queued.start(), estimate.nanos())));
                }
                availableBundles.add(finish);
                shortfall = Math.max(shortfall, queued.shortfall());
            }
            int lanes = Math.min(bundleCapacity, Math.max(1, ordered.size() + active.size()));
            availableBundles.sort(Comparator.naturalOrder());
            if (availableBundles.size() > lanes) {
                availableBundles = new ArrayList<>(availableBundles.subList(availableBundles.size() - lanes,
                        availableBundles.size()));
            }
            while (availableBundles.size() < lanes) {
                availableBundles.add(now);
            }
            long[] laneEnd = new long[lanes];
            Arrays.fill(laneEnd, workCutoff);
            for (int i = ordered.size() - 1; i >= 0; i--) {
                Job job = ordered.get(i);
                long critical = Math.max(job.minimumDurationNanos(), job.topics() == 0 ? 0
                        : Math.max(estimate.nanos(), job.longestTopicNanos()));
                double work = job.topics() == 0 ? 0 : (job.topics() - 1) * (double) estimate.nanos()
                        + Math.max(estimate.nanos(), job.longestTopicNanos());
                long duration = Math.max(critical, nanos(work / topicCapacity));
                int lane = chooseLane(laneEnd, availableBundles, duration);
                long firstSlot = capacity.free.isEmpty() ? Long.MAX_VALUE : capacity.free.firstKey();
                Allocation allocation = capacity.backward(work, laneEnd[lane]);
                long latestStart = Math.min(allocation.start(), subtract(laneEnd[lane], duration));
                if (work > 0 && latestStart < firstSlot) {
                    shortfall = Math.max(shortfall, difference(firstSlot, latestStart));
                }
                shortfall = Math.max(shortfall, allocation.shortfall());
                if (latestStart < availableBundles.get(lane)) {
                    shortfall = Math.max(shortfall, difference(availableBundles.get(lane), latestStart));
                }
                latest.put(job.id(), latestStart);
                laneEnd[lane] = latestStart;
            }
            if (shortfall > 0 || (now > workCutoff && (!ordered.isEmpty() || !active.isEmpty()
                    || !backgroundTopicRemainingNanos.isEmpty()))) {
                exhaustion.add(Reason.CAPACITY);
            }
        }
        double scale = 1;
        for (double impact : started.values()) {
            scale = Math.max(scale, impact);
        }
        for (Job job : ordered) {
            scale = Math.max(scale, job.idle() ? 0 : job.impact());
        }
        double prefix = 0;
        for (double impact : started.values()) {
            prefix += impact / scale;
        }
        double total = prefix;
        for (Job job : ordered) {
            total += job.idle() ? 0 : job.impact() / scale;
        }
        List<Scheduled> planned = new ArrayList<>();
        for (Job job : ordered) {
            long nominal = unbounded || job.idle() || total == 0 ? 0 : nanos(workCutoff * (prefix / total));
            long latestStart = latest.getOrDefault(job.id(), Long.MAX_VALUE);
            long due = unbounded || !exhaustion.isEmpty() || job.idle() ? now : Math.min(nominal, latestStart);
            Long previous = previousDue.get(job.id());
            if (previous != null && previous <= now) {
                due = Math.min(previous, due);
            }
            planned.add(new Scheduled(job, nominal, latestStart, due, due));
            prefix += job.idle() ? 0 : job.impact() / scale;
        }
        planned = applyRateCap(planned);
        if (!unbounded && startInterval > 0) {
            for (Scheduled job : planned) {
                if (job.notBeforeNanos() > job.latestNanos()) {
                    exhaustion.add(Reason.RATE_CAP);
                    shortfall = Math.max(shortfall, difference(job.notBeforeNanos(), job.latestNanos()));
                }
            }
        }
        if (!exhaustion.isEmpty()) {
            List<Scheduled> ready = new ArrayList<>();
            for (Scheduled job : planned) {
                long due = Math.min(now, job.dueNanos());
                ready.add(new Scheduled(job.job(), job.nominalNanos(), job.latestNanos(), due, due));
            }
            planned = applyRateCap(ready);
        }
        previousDue.keySet().retainAll(identities);
        planned.forEach(job -> previousDue.put(job.job().id(), job.dueNanos()));
        return new Plan(List.copyOf(planned), unbounded || !exhaustion.isEmpty(),
                !previouslyExhausted && !exhaustion.isEmpty(), Set.copyOf(exhaustion), shortfall);
    }

    private List<Scheduled> applyRateCap(List<Scheduled> jobs) {
        jobs.sort(Comparator.comparingLong(Scheduled::dueNanos)
                .thenComparing(job -> job.job().idle()).thenComparing(job -> job.job().id()));
        List<Scheduled> result = new ArrayList<>();
        long next = lastStart < 0 ? 0 : add(lastStart, startInterval);
        for (Scheduled job : jobs) {
            long allowed = startInterval == 0 ? job.dueNanos() : Math.max(next, job.dueNanos());
            result.add(new Scheduled(job.job(), job.nominalNanos(), job.latestNanos(), job.dueNanos(), allowed));
            next = add(allowed, startInterval);
        }
        return result;
    }

    private static int chooseLane(long[] ends, List<Long> available, long duration) {
        int best = -1;
        for (int i = 0; i < ends.length; i++) {
            if (subtract(ends[i], duration) >= available.get(i)
                    && (best < 0 || ends[i] > ends[best]
                    || (ends[i] == ends[best] && available.get(i) < available.get(best)))) {
                best = i;
            }
        }
        if (best >= 0) {
            return best;
        }
        best = 0;
        for (int i = 1; i < ends.length; i++) {
            if (ends[i] > ends[best] || (ends[i] == ends[best] && available.get(i) < available.get(best))) {
                best = i;
            }
        }
        return best;
    }

    private static long add(long first, long second) {
        return second > Long.MAX_VALUE - first ? Long.MAX_VALUE : first + second;
    }

    private static long subtract(long first, long second) {
        return first < Long.MIN_VALUE + second ? Long.MIN_VALUE : first - second;
    }

    private static long difference(long end, long start) {
        return start < 0 && end > Long.MAX_VALUE + start ? Long.MAX_VALUE : Math.max(0, end - start);
    }

    private static long nanos(double value) {
        return value >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0, (long) Math.ceil(value));
    }

    private record Allocation(long start, long finish, long shortfall) { }

    private record Segment(long start, long end, int capacity) { }

    /** Each full segment is consumed once; partial reservations split at most two segments per job. */
    private static final class CapacityTimeline {
        private final TreeMap<Long, Segment> free = new TreeMap<>();
        private final long now;
        private final long cutoff;
        private final int capacity;

        private CapacityTimeline(long now, long cutoff, int capacity, List<Long> occupied) {
            this.now = now;
            this.cutoff = cutoff;
            this.capacity = capacity;
            TreeMap<Long, Integer> releases = new TreeMap<>();
            int busy = 0;
            for (long remaining : occupied) {
                if (remaining > 0) {
                    busy++;
                    long until = add(now, remaining);
                    if (until < cutoff) {
                        releases.merge(until, 1, Integer::sum);
                    }
                }
            }
            long start = now;
            for (var event : releases.entrySet()) {
                put(start, event.getKey(), Math.max(0, capacity - busy));
                busy -= event.getValue();
                start = event.getKey();
            }
            put(start, cutoff, Math.max(0, capacity - busy));
        }

        private void put(long start, long end, int slots) {
            if (start < end && slots > 0) {
                free.put(start, new Segment(start, end, slots));
            }
        }

        private Allocation forward(double work) {
            long start = now;
            long finish = now;
            boolean first = true;
            while (work > 0 && !free.isEmpty()) {
                Segment segment = free.firstEntry().getValue();
                if (first) {
                    start = segment.start();
                    first = false;
                }
                double available = (segment.end() - segment.start()) * (double) segment.capacity();
                free.remove(segment.start());
                if (work >= available) {
                    work -= available;
                    finish = segment.end();
                } else {
                    finish = add(segment.start(), nanos(work / segment.capacity()));
                    put(finish, segment.end(), segment.capacity());
                    work = 0;
                }
            }
            long shortfall = nanos(work / capacity);
            return new Allocation(start, shortfall == 0 ? finish : add(cutoff, shortfall), shortfall);
        }

        private Allocation backward(double work, long before) {
            long start = before;
            long finish = before;
            while (work > 0) {
                var entry = free.lowerEntry(before);
                if (entry == null) {
                    break;
                }
                Segment segment = entry.getValue();
                long end = Math.min(before, segment.end());
                double available = (end - segment.start()) * (double) segment.capacity();
                free.remove(segment.start());
                put(end, segment.end(), segment.capacity());
                if (work >= available) {
                    work -= available;
                    start = segment.start();
                    before = start;
                } else {
                    start = end - nanos(work / segment.capacity());
                    put(segment.start(), start, segment.capacity());
                    work = 0;
                }
            }
            long shortfall = nanos(work / capacity);
            return new Allocation(shortfall == 0 ? start : now - shortfall, finish, shortfall);
        }
    }
}
