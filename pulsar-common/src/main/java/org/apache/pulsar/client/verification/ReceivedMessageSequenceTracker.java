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
package org.apache.pulsar.client.verification;

/**
 * Implementation of {@link AbstractReceivedMessageSequenceTracker} that uses
 * fields to track the count of lost messages, duplicated messages and out-of-order messages
 *
 * This class is not thread safe.
 */
public class ReceivedMessageSequenceTracker extends AbstractReceivedMessageSequenceTracker {
    private long lostMessages;
    private long duplicatedMessages;
    private long outOfOrderMessages;

    public ReceivedMessageSequenceTracker() {
        super();
    }

    public ReceivedMessageSequenceTracker(int maxTrackOutOfOrderSequenceNumbers, int maxTrackSkippedSequenceNumber) {
        super(maxTrackOutOfOrderSequenceNumbers, maxTrackSkippedSequenceNumber);
    }

    @Override
    protected void messageLossDetected() {
        lostMessages++;
    }

    @Override
    protected void messageDuplicationDetected() {
        duplicatedMessages++;
    }

    @Override
    protected void lateOutOfOrderDeliveryDetected() {
        lostMessages--;
        outOfOrderMessages++;
    }

    @Override
    protected void outOfOrderDeliveryDetected() {
        outOfOrderMessages++;
    }

    public long getLostMessages() {
        return lostMessages;
    }

    public long getDuplicatedMessages() {
        return duplicatedMessages;
    }

    public long getOutOfOrderMessages() {
        return outOfOrderMessages;
    }
}
