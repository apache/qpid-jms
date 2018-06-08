/*
 *
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
 *
 */
package org.apache.qpid.jms.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.AmqpPeerRunnable;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdleTimeoutIntegrationTest extends QpidJmsTestCase {

    public static final Logger LOGGER = LoggerFactory.getLogger(IdleTimeoutIntegrationTest.class);

    @Test(timeout = 20000)
    public void testIdleTimeoutIsAdvertisedByDefault() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(null, greaterThan(UnsignedInteger.valueOf(0)), null, false);
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testAdvertisedIdleTimeoutIsHalfOfActualTimeoutValue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            int configuredTimeout = 54320;
            int advertisedValue = configuredTimeout / 2;

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen(null, greaterThan(UnsignedInteger.valueOf(advertisedValue)), null, false);

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?amqp.idleTimeout=" + configuredTimeout);
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            testPeer.expectClose();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testClientSendsEmptyFramesWhenPeerAdvertisesIdleTimeout() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            int period = 20;
            int idleSleep =  100;
            int advertisedTimeout = period * 2;

            testPeer.setAdvertisedIdleTimeout(advertisedTimeout);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            // Sleep for a bit, let the idle handling work
            Thread.sleep(idleSleep);

            testPeer.expectClose();

            connection.close();

            // Verify that *any* empty frames were received by the peer.
            // We will verify additional behaviours with slower tests.
            assertThat(testPeer.getEmptyFrameCount(), Matchers.greaterThan(0));
        }
    }

    //TODO: Could use JUnit categories to make this slowish test skipable?
    //      If so, make it slower still and more granular.
    @Test(timeout = 20000)
    public void testClientSendsEmptyFramesWithExpectedFrequency() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            int period = 250;
            int advertisedTimeout = period * 2;
            int cycles = 10;
            int idleSleep =  cycles * period;
            int offset = 2;

            testPeer.setAdvertisedIdleTimeout(advertisedTimeout);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort());
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());

            // Sleep for a bit, let the idle handling work
            Thread.sleep(idleSleep);

            testPeer.expectClose();

            connection.close();

            assertThat(testPeer.getEmptyFrameCount(), Matchers.greaterThanOrEqualTo(cycles - offset));
            assertThat(testPeer.getEmptyFrameCount(), Matchers.lessThanOrEqualTo(cycles + offset));
        }
    }

    @Test(timeout = 20000)
    public void testConnectionSetFailedWhenPeerNeglectsToSendEmptyFrames() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            int configuredTimeout = 200;

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?amqp.idleTimeout=" + configuredTimeout);
            final JmsConnection connection = (JmsConnection) factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForAllHandlersToComplete(1000);
            // The peer is still connected, so it will get the close frame with error
            testPeer.expectClose(Matchers.notNullValue(), false);
            assertNull(testPeer.getThrowable());
            testPeer.setSuppressReadExceptionOnClose(true);

            boolean failed = Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return connection.isFailed();
                }
            }, 10000, 10);

            assertTrue("connection didnt fail in expected timeframe", failed);
            testPeer.waitForAllHandlersToComplete(1000);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testConnectionNotMarkedFailedWhenPeerSendsEmptyFrames() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            int configuredTimeout = 2000;
            int period = 500;
            int cycles = 6;

            final CountDownLatch latch = new CountDownLatch(cycles);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();

            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin();

            // Start to emit idle frames when the connection is set up, this should stop it timing out
            testPeer.runAfterLastHandler(new EmptyFrameSender(latch, period, cycles, testPeer));

            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:" + testPeer.getServerPort() + "?amqp.idleTimeout=" + configuredTimeout);
            final JmsConnection connection = (JmsConnection) factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            boolean framesSent = latch.await(cycles * period * 2, TimeUnit.MILLISECONDS);
            assertTrue("idle frames were not sent as expected", framesSent);

            assertFalse("connection shouldnt fail", connection.isFailed());
            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
            assertNull(testPeer.getThrowable());
        }
    }

    private static class EmptyFrameSender implements AmqpPeerRunnable
    {
        private int delay;
        private int cycles;
        private CountDownLatch latch;
        private TestAmqpPeer testPeer;

        public EmptyFrameSender(CountDownLatch latch, int delay, int cycles, TestAmqpPeer testPeer) {
            this.cycles = cycles;
            this.delay = delay;
            this.latch = latch;
            this.testPeer = testPeer;
        }

        @Override
        public void run() {
            for (int i = 0; i < cycles; i++) {
                LOGGER.info("Delaying before empty frame: {}", i + 1);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    // pass
                }
                LOGGER.info("Sending empty frame: {}", i + 1);
                testPeer.sendEmptyFrame(false);
                latch.countDown();
            }
        }
    }
}
