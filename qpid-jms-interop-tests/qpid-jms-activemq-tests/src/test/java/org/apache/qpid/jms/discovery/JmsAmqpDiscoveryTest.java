/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.discovery;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsConnectionListener;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.apache.qpid.jms.support.Wait;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that a Broker using AMQP can be discovered and JMS operations can be performed.
 */
public class JmsAmqpDiscoveryTest extends AmqpTestSupport implements JmsConnectionListener {

    private static final Logger LOG = LoggerFactory.getLogger(JmsAmqpDiscoveryTest.class);

    private CountDownLatch interrupted;
    private CountDownLatch restored;
    private JmsConnection jmsConnection;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        interrupted = new CountDownLatch(1);
        restored = new CountDownLatch(1);
    }

    @Test(timeout=60000)
    public void testRunningBrokerIsDiscovered() throws Exception {
        connection = createConnection();
        connection.start();

        assertTrue("connection never connected.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return jmsConnection.isConnected();
            }
        }));
    }

    @Test(timeout=60000)
    public void testConnectionFailsWhenBrokerGoesDown() throws Exception {
        connection = createConnection();
        connection.start();

        assertTrue("connection never connected.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return jmsConnection.isConnected();
            }
        }));

        LOG.info("Connection established, stopping broker.");
        stopPrimaryBroker();

        assertTrue("Interrupted event never fired", interrupted.await(30, TimeUnit.SECONDS));
    }

    @Test(timeout=60000)
    public void testConnectionRestoresAfterBrokerRestarted() throws Exception {
        connection = createConnection();
        connection.start();

        assertTrue("connection never connected.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return jmsConnection.isConnected();
            }
        }));

        stopPrimaryBroker();
        assertTrue(interrupted.await(20, TimeUnit.SECONDS));
        startPrimaryBroker();
        assertTrue(restored.await(20, TimeUnit.SECONDS));
    }

    @Test(timeout=60000)
    public void testDiscoversAndReconnectsToSecondaryBroker() throws Exception {

        connection = createConnection();
        connection.start();

        assertTrue("connection never connected.", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return jmsConnection.isConnected();
            }
        }));

        startNewBroker();
        stopPrimaryBroker();

        assertTrue(interrupted.await(20, TimeUnit.SECONDS));
        assertTrue(restored.await(20, TimeUnit.SECONDS));
    }

    @Override
    protected boolean isAmqpDiscovery() {
        return true;
    }

    protected Connection createConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "discovery:(multicast://default)?maxReconnectDelay=500");
        connection = factory.createConnection();
        jmsConnection = (JmsConnection) connection;
        jmsConnection.addConnectionListener(this);
        return connection;
    }

    @Override
    public void onConnectionFailure(Throwable error) {
        LOG.info("Connection reported failover: {}", error.getMessage());
    }

    @Override
    public void onConnectionInterrupted(URI remoteURI) {
        LOG.info("Connection reports interrupted. Lost connection to -> {}", remoteURI);
        interrupted.countDown();
    }

    @Override
    public void onConnectionRestored(URI remoteURI) {
        LOG.info("Connection reports restored.  Connected to -> {}", remoteURI);
        restored.countDown();
    }

    @Override
    public void onMessage(JmsInboundMessageDispatch envelope) {
    }
}
