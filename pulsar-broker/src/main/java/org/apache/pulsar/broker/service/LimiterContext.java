package org.apache.pulsar.broker.service;

public interface LimiterContext {
    Limiter getLimiter();
    void registerLimitCleanup(Runnable runnable);
}
