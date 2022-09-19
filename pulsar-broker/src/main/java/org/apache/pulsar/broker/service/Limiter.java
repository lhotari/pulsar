package org.apache.pulsar.broker.service;

public interface Limiter {
    boolean isLimitExceeded();
}
