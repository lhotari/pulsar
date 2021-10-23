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

import static org.testng.Assert.assertEquals;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReceivedMessageSequenceTrackerTest {
    ReceivedMessageSequenceTracker messageSequenceTracker;

    @BeforeMethod
    void setup() {
        messageSequenceTracker = new ReceivedMessageSequenceTracker(20, 20);
    }

    @Test
    void shouldCountersBeZeroWhenSequenceDoesntContainGaps() {
        // when
        for (long l = 0; l < 100L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        messageSequenceTracker.close();
        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 0);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 0);
        assertEquals(messageSequenceTracker.getLostMessages(), 0);
    }

    @DataProvider(name = "totalMessages")
    public Object[][] totalMessages() {
        return new Object[][] {
                {10L}, {11L}, {19L}, {20L}, {21L}, {100L},
        };
    }

    @DataProvider(name = "totalMessages21st")
    public Object[][] totalMessages21st() {
        return new Object[][] {
                {20L}, {21L}, {40L}, {41L}, {42L}, {43L}, {100L},
        };
    }

    @Test(dataProvider = "totalMessages")
    void shouldDetectMsgLossWhenEverySecondMessageIsLost(long totalMessages) {
        doShouldDetectMsgLoss(totalMessages, 2);
    }

    @Test(dataProvider = "totalMessages")
    void shouldDetectMsgLossWhenEveryThirdMessageIsLost(long totalMessages) {
        doShouldDetectMsgLoss(totalMessages, 3);
    }

    @Test(dataProvider = "totalMessages21st")
    void shouldDetectMsgLossWhenEvery21stMessageIsLost(long totalMessages) {
        doShouldDetectMsgLoss(totalMessages, 21);
    }

    private void doShouldDetectMsgLoss(long totalMessages, int looseEveryNthMessage) {
        int messagesLost = 0;
        // when
        boolean lastMessageWasLost = false;
        for (long l = 0; l < totalMessages; l++) {
            if (l % looseEveryNthMessage == 1) {
                messagesLost++;
                lastMessageWasLost = true;
                continue;
            } else {
                lastMessageWasLost = false;
            }
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        if (lastMessageWasLost) {
            messageSequenceTracker.sequenceNumberReceived(totalMessages);
        }
        messageSequenceTracker.close();
        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 0);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 0);
        assertEquals(messageSequenceTracker.getLostMessages(), messagesLost);
    }

    @Test(dataProvider = "totalMessages")
    void shouldDetectMsgDuplication(long totalMessages) {
        int messagesDuplicated = 0;
        // when
        for (long l = 0; l < totalMessages; l++) {
            if (l % 2 == 1) {
                messagesDuplicated++;
                messageSequenceTracker.sequenceNumberReceived(l);
            }
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        if (totalMessages % 2 == 0) {
            messageSequenceTracker.sequenceNumberReceived(totalMessages);
        }
        if (totalMessages < 2 * messageSequenceTracker.getMaxTrackOutOfOrderSequenceNumbers()) {
            messageSequenceTracker.close();
        }

        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 0);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), messagesDuplicated);
        assertEquals(messageSequenceTracker.getLostMessages(), 0);
    }

    @Test
    void shouldDetectSingleMessageOutOfSequence() {
        // when
        for (long l = 0; l < 10L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        messageSequenceTracker.sequenceNumberReceived(10L);
        messageSequenceTracker.sequenceNumberReceived(12L);
        messageSequenceTracker.sequenceNumberReceived(11L);
        for (long l = 13L; l < 100L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }

        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 1);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 0);
        assertEquals(messageSequenceTracker.getLostMessages(), 0);
    }

    @Test
    void shouldDetectMultipleMessagesOutOfSequence() {
        // when
        for (long l = 0; l < 10L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        messageSequenceTracker.sequenceNumberReceived(10L);
        messageSequenceTracker.sequenceNumberReceived(14L);
        messageSequenceTracker.sequenceNumberReceived(13L);
        messageSequenceTracker.sequenceNumberReceived(11L);
        messageSequenceTracker.sequenceNumberReceived(12L);
        for (long l = 15L; l < 100L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }

        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 2);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 0);
        assertEquals(messageSequenceTracker.getLostMessages(), 0);
    }

    @Test
    void shouldDetectIndividualMessageLoss() {
        // when
        for (long l = 0; l < 100L; l++) {
            if (l != 11L) {
                messageSequenceTracker.sequenceNumberReceived(l);
            }
        }
        messageSequenceTracker.close();

        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 0);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 0);
        assertEquals(messageSequenceTracker.getLostMessages(), 1);
    }

    @Test
    void shouldDetectGapAndMessageDuplication() {
        // when
        for (long l = 0; l < 100L; l++) {
            if (l != 11L) {
                messageSequenceTracker.sequenceNumberReceived(l);
            }
            if (l == 12L) {
                messageSequenceTracker.sequenceNumberReceived(l);
            }
        }
        messageSequenceTracker.close();

        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 0);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 1);
        assertEquals(messageSequenceTracker.getLostMessages(), 1);
    }

    @Test
    void shouldDetectGapAndMessageDuplicationTimes2() {
        // when
        for (long l = 0; l < 100L; l++) {
            if (l != 11L) {
                messageSequenceTracker.sequenceNumberReceived(l);
            }
            if (l == 12L) {
                messageSequenceTracker.sequenceNumberReceived(l);
                messageSequenceTracker.sequenceNumberReceived(l);
            }
        }
        messageSequenceTracker.close();

        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 0);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 2);
        assertEquals(messageSequenceTracker.getLostMessages(), 1);
    }


    @Test
    void shouldDetectDelayedOutOfOrderDelivery() {
        // when
        for (long l = 0; l < 5 * messageSequenceTracker.getMaxTrackOutOfOrderSequenceNumbers(); l++) {
            if (l != 10) {
                messageSequenceTracker.sequenceNumberReceived(l);
            }
            if (l == messageSequenceTracker.getMaxTrackOutOfOrderSequenceNumbers() * 2) {
                messageSequenceTracker.sequenceNumberReceived(10);
            }
        }
        messageSequenceTracker.close();

        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 1);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 0);
        assertEquals(messageSequenceTracker.getLostMessages(), 0);
    }

    @Test
    void shouldDetectDelayedOutOfOrderDeliveryOf2ConsecutiveSequenceNumbers() {
        // when
        for (long l = 0; l < 5 * messageSequenceTracker.getMaxTrackOutOfOrderSequenceNumbers(); l++) {
            if (l != 10 && l != 11) {
                messageSequenceTracker.sequenceNumberReceived(l);
            }
            if (l == messageSequenceTracker.getMaxTrackOutOfOrderSequenceNumbers() * 2) {
                messageSequenceTracker.sequenceNumberReceived(10);
                messageSequenceTracker.sequenceNumberReceived(11);
            }
        }
        messageSequenceTracker.close();

        // then
        assertEquals(messageSequenceTracker.getOutOfOrderMessages(), 2);
        assertEquals(messageSequenceTracker.getDuplicatedMessages(), 0);
        assertEquals(messageSequenceTracker.getLostMessages(), 0);
    }
}