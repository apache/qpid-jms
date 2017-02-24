/*
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
package org.apache.qpid.jms.provider.failover;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.NETWORK_HOST;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.OPEN_HOSTNAME;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PORT;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.ConnectionError;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that when failover is used and the remote sends a redirect error, the
 * failover transport obtains the new peer connection info and attempts to connect
 * there.
 */
public class FailoverRedirectTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverRedirectTest.class);

    @Test(timeout = 40000)
    public void testFailoverHandlesRedirection() throws Exception {
        try (TestAmqpPeer rejectingPeer = new TestAmqpPeer();
             TestAmqpPeer redirectedPeer = new TestAmqpPeer();) {

            final CountDownLatch connected = new CountDownLatch(1);
            final URI redirectURI = createPeerURI(redirectedPeer);
            LOG.info("Backup peer is at: {}", redirectURI);

            redirectedPeer.expectSaslAnonymous();
            redirectedPeer.expectOpen();
            redirectedPeer.expectBegin();

            Map<Symbol, Object> redirectInfo = new HashMap<Symbol, Object>();
            redirectInfo.put(OPEN_HOSTNAME, "localhost");
            redirectInfo.put(NETWORK_HOST, "localhost");
            redirectInfo.put(PORT, redirectedPeer.getServerPort());

            rejectingPeer.rejectConnect(ConnectionError.REDIRECT, "Server is full, go away", redirectInfo);

            final JmsConnection connection = establishAnonymousConnecton(rejectingPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(redirectURI, remoteURI)) {
                        connected.countDown();
                    }
                }
            });
            connection.start();

            rejectingPeer.waitForAllHandlersToComplete(1000);
            assertTrue("Should connect to backup peer", connected.await(15, TimeUnit.SECONDS));

            redirectedPeer.expectClose();
            connection.close();
            redirectedPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 40000)
    public void testFailoverHandlesRemotelyEndConnectionWithRedirection() throws Exception {
        try (TestAmqpPeer rejectingPeer = new TestAmqpPeer();
             TestAmqpPeer redirectedPeer = new TestAmqpPeer();) {

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);
            final CountDownLatch connectedToBackup = new CountDownLatch(1);

            final URI rejectingURI = createPeerURI(rejectingPeer);
            final URI redirectURI = createPeerURI(redirectedPeer);
            LOG.info("Primary is at {}: Backup peer is at: {}", rejectingURI, redirectURI);

            redirectedPeer.expectSaslAnonymous();
            redirectedPeer.expectOpen();
            redirectedPeer.expectBegin();

            Map<Symbol, Object> redirectInfo = new HashMap<Symbol, Object>();
            redirectInfo.put(OPEN_HOSTNAME, "localhost");
            redirectInfo.put(NETWORK_HOST, "localhost");
            redirectInfo.put(PORT, redirectedPeer.getServerPort());

            rejectingPeer.expectSaslAnonymous();
            rejectingPeer.expectOpen();
            rejectingPeer.expectBegin();
            rejectingPeer.remotelyCloseConnection(true, ConnectionError.REDIRECT, "Server is full, go away", redirectInfo);

            final JmsConnection connection = establishAnonymousConnecton(rejectingPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(rejectingURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                    if (isExpectedHost(redirectURI, remoteURI)) {
                        connectedToBackup.countDown();
                    }
                }
            });
            connection.start();

            rejectingPeer.waitForAllHandlersToComplete(1000);

            assertTrue("Should connect to primary peer", connectedToPrimary.await(15, TimeUnit.SECONDS));
            assertTrue("Should connect to backup peer", connectedToBackup.await(15, TimeUnit.SECONDS));

            redirectedPeer.expectClose();
            connection.close();
            redirectedPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private JmsConnection establishAnonymousConnecton(TestAmqpPeer testPeer) throws Exception {
        final String remoteURI = "failover:(" + createPeerURI(testPeer).toString() + ")";

        ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
        Connection connection = factory.createConnection();

        return (JmsConnection) connection;
    }

    private boolean isExpectedHost(URI expected, URI actual) {
        if (!expected.getHost().equals(actual.getHost())) {
            LOG.info("Expected host {} but got host {}", expected.getHost(), actual.getHost());
            return false;
        }

        if (expected.getPort() != actual.getPort()) {
            LOG.info("Expected host {} on port {} but got host {} on port {}",
                expected.getHost(), expected.getPort(), actual.getHost(), actual.getPort());
            return false;
        }

        return true;
    }

    private URI createPeerURI(TestAmqpPeer peer) throws URISyntaxException {
        return new URI("amqp://localhost:" + peer.getServerPort());
    }
}
