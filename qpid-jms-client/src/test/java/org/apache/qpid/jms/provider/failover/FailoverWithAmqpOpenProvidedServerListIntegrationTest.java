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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SCHEME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.jms.util.URISupport;
import org.apache.qpid.proton.amqp.Symbol;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverWithAmqpOpenProvidedServerListIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverWithAmqpOpenProvidedServerListIntegrationTest.class);

    private static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    private static final String PASSWORD = "password";
    private static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";

    private static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    private static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    private static final Symbol FAILOVER_SERVER_LIST = Symbol.valueOf("failover-server-list");
    private static final Symbol NETWORK_HOST = Symbol.valueOf("network-host");
    private static final Symbol HOSTNAME = Symbol.valueOf("hostname");
    private static final Symbol PORT =  Symbol.valueOf("port");

    /*
     * Verify that when the Open frame contains a failover server list, and the client is configured to
     * replace the servers in its existing URI pool, it does so, leaving the server successfully connected
     * to plus the announced failover servers.
     */
    @Test(timeout = 20000)
    public void testFailoverHandlesServerProvidedFailoverListReplace() throws Exception {
        doFailoverHandlesServerProvidedFailoverListTestImpl(true);
    }

    /*
     * Verify that when the Open frame contains a failover server list, and the client is configured to
     * add the servers to its existing URI pool, it does so.
     */
    @Test(timeout = 20000)
    public void testFailoverHandlesServerProvidedFailoverListAdd() throws Exception {
        doFailoverHandlesServerProvidedFailoverListTestImpl(false);
    }

    private void doFailoverHandlesServerProvidedFailoverListTestImpl(boolean replace) throws Exception {
        try (TestAmqpPeer primaryPeer = new TestAmqpPeer();
             TestAmqpPeer backupPeer1 = new TestAmqpPeer();
             TestAmqpPeer backupPeer2 = new TestAmqpPeer();) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            final URI backupPeer1URI = createPeerURI(backupPeer1);
            final URI backupPeer2URI = createPeerURI(backupPeer2);
            LOG.info("Primary is at: {}", primaryPeerURI);
            LOG.info("Backup1 is at: {}", backupPeer1URI);
            LOG.info("Backup2 is at: {}", backupPeer2URI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);
            final CountDownLatch connectedToBackup1 = new CountDownLatch(1);
            final CountDownLatch connectedToBackup2 = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is created
            primaryPeer.expectSaslAnonymous();

            String failoverParams = null;
            if (replace) {
                failoverParams = "?failover.maxReconnectAttempts=10&failover.amqpOpenServerListAction=REPLACE";
            } else {
                failoverParams = "?failover.maxReconnectAttempts=10&failover.amqpOpenServerListAction=ADD";
            }

            // We only give it the primary/dropping peer details. It can only connect to the backup
            // peer by identifying the details in the announced failover-server-list.
            final JmsConnection connection = establishAnonymousConnecton(failoverParams, primaryPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                    if (isExpectedHost(backupPeer1URI, remoteURI)) {
                        connectedToBackup1.countDown();
                    } else if (isExpectedHost(backupPeer2URI, remoteURI)) {
                        connectedToBackup2.countDown();
                    }
                }
            });

            // Verify the existing failover URIs are as expected, the initial peer only
            List<URI> beforeOpenFailoverURIs = new ArrayList<>();
            beforeOpenFailoverURIs.add(primaryPeerURI);

            assertFailoverURIList(connection, beforeOpenFailoverURIs);

            // Set the primary up to expect the connection, have the failover list containing the backup1 advertised
            Map<Symbol,Object> backupPeer1Details = new HashMap<>();
            backupPeer1Details.put(NETWORK_HOST, "localhost");
            backupPeer1Details.put(PORT, backupPeer1.getServerPort());

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(backupPeer1Details);

            Map<Symbol,Object> server1ConnectionProperties = new HashMap<Symbol, Object>();
            server1ConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(server1ConnectionProperties);
            primaryPeer.expectBegin();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected, now containing initial peer and the backup1
            List<URI> afterOpenFailoverURIs = new ArrayList<>();
            afterOpenFailoverURIs.add(primaryPeerURI);
            afterOpenFailoverURIs.add(backupPeer1URI);

            assertFailoverURIList(connection, afterOpenFailoverURIs);

            // Set the backup1 to expect a connection, have the failover list containing the backup2 advertised
            Map<Symbol,Object> backupPeer2Details = new HashMap<>();
            backupPeer2Details.put(NETWORK_HOST, "localhost");
            backupPeer2Details.put(PORT, backupPeer2.getServerPort());

            List<Map<Symbol, Object>> backup1FailoverServerList = new ArrayList<Map<Symbol, Object>>();
            backup1FailoverServerList.add(backupPeer2Details);

            Map<Symbol,Object> backup1serverConnectionProperties = new HashMap<Symbol, Object>();
            backup1serverConnectionProperties.put(FAILOVER_SERVER_LIST, backup1FailoverServerList);

            backupPeer1.expectSaslAnonymous();
            backupPeer1.expectOpen(backup1serverConnectionProperties);
            backupPeer1.expectBegin();

            // Kill the primary peer
            primaryPeer.close();

            assertTrue("Should connect to backup1 peer", connectedToBackup1.await(5, TimeUnit.SECONDS));
            assertEquals("Should not yet connect to backup2 peer", 1, connectedToBackup2.getCount());

            // Verify the failover URIs are as expected
            List<URI> afterFirstReconnectFailoverURIs = new ArrayList<>();
            if (replace) {
                // Now containing backup1 and backup2 peers
                afterFirstReconnectFailoverURIs.add(backupPeer1URI);
                afterFirstReconnectFailoverURIs.add(backupPeer2URI);
            } else {
                // Now containing primary, backup1, and backup2 peers
                afterFirstReconnectFailoverURIs.add(primaryPeerURI);
                afterFirstReconnectFailoverURIs.add(backupPeer1URI);
                afterFirstReconnectFailoverURIs.add(backupPeer2URI);
            }

            assertFailoverURIList(connection, afterFirstReconnectFailoverURIs);

            // Set the backup2 to expect a connection
            backupPeer2.expectSaslAnonymous();
            backupPeer2.expectOpen();
            backupPeer2.expectBegin();

            // Kill the backup1 peer
            backupPeer1.close();

            assertTrue("Should connect to backup2 peer", connectedToBackup2.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected
            List<URI> afterSecondReconnectFailoverURIs = new ArrayList<>();
            if (replace) {
                // Still containing backup1 and backup2 peers
                afterSecondReconnectFailoverURIs.add(backupPeer1URI);
                afterSecondReconnectFailoverURIs.add(backupPeer2URI);
            } else {
                // Still containing primary, backup1, and backup2 peers
                afterSecondReconnectFailoverURIs.add(primaryPeerURI);
                afterSecondReconnectFailoverURIs.add(backupPeer1URI);
                afterSecondReconnectFailoverURIs.add(backupPeer2URI);
            }

            assertFailoverURIList(connection, afterSecondReconnectFailoverURIs);

            backupPeer2.expectClose();
            connection.close();
            backupPeer2.waitForAllHandlersToComplete(1000);
        }
    }

    /*
     * Verify that when the Open frame contains a failover server list, and the client is configured to ignore it,
     * no change occurs in the failover URIs in use by the client after connecting.
     */
    @Test(timeout = 20000)
    public void testFailoverHandlesServerProvidedFailoverListIgnore() throws Exception {
        try (TestAmqpPeer primaryPeer = new TestAmqpPeer();) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            LOG.info("Peer is at: {}", primaryPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is created
            primaryPeer.expectSaslAnonymous();

            String failoverParams = "?failover.maxReconnectAttempts=10&failover.amqpOpenServerListAction=IGNORE";

            // We only give it the primary peer details. It can only connect to the backup
            // peer by identifying the details in the announced failover-server-list.
            final JmsConnection connection = establishAnonymousConnecton(failoverParams, primaryPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }
            });

            // Verify the existing failover URIs are as expected, the initial peer only
            List<URI> primaryPeerOnlyFailoverURIs = new ArrayList<>();
            primaryPeerOnlyFailoverURIs.add(primaryPeerURI);

            assertFailoverURIList(connection, primaryPeerOnlyFailoverURIs);

            // Set the primary up to expect the connection, have the failover list containing another server
            Map<Symbol,Object> otherPeerDetails = new HashMap<>();
            otherPeerDetails.put(NETWORK_HOST, "testhost");
            otherPeerDetails.put(PORT, "4567");

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(otherPeerDetails);

            Map<Symbol,Object> serverConnectionProperties = new HashMap<Symbol, Object>();
            serverConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(serverConnectionProperties);
            primaryPeer.expectBegin();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the existing failover URIs are as expected, still the initial peer only
            assertFailoverURIList(connection, primaryPeerOnlyFailoverURIs);

            primaryPeer.expectClose();
            connection.close();
            primaryPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /*
     * Verify that when the Open frame contains a failover server list, and it specifies an AMQP hostname in
     * a particular servers details, the hostname is used when failover occurs.
     */
    @Test(timeout = 20000)
    public void testFailoverHandlesServerProvidedFailoverListWithHostname() throws Exception {
        try (TestAmqpPeer primaryPeer = new TestAmqpPeer();
             TestAmqpPeer backupPeer = new TestAmqpPeer();) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            final URI backupPeerURI = createPeerURI(backupPeer);
            LOG.info("Primary is at: {}", primaryPeerURI);
            LOG.info("Backup is at: {}", backupPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);
            final CountDownLatch connectedToBackup = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is created
            primaryPeer.expectSaslAnonymous();

            // We only give it the primary/dropping peer details. It can only connect to the backup
            // peer by identifying the details in the announced failover-server-list.
            final JmsConnection connection = establishAnonymousConnecton(null, primaryPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                    if (isExpectedHost(backupPeerURI, remoteURI)) {
                        connectedToBackup.countDown();
                    }
                }
            });

            // Verify the existing failover URIs are as expected, the initial peer only
            List<URI> beforeOpenFailoverURIs = new ArrayList<>();
            beforeOpenFailoverURIs.add(primaryPeerURI);

            assertFailoverURIList(connection, beforeOpenFailoverURIs);

            // Set the primary up to expect the connection, have the failover list containing the backup1 advertised
            Map<Symbol,Object> backupPeer1Details = new HashMap<>();
            backupPeer1Details.put(NETWORK_HOST, "localhost");
            backupPeer1Details.put(PORT, backupPeer.getServerPort());
            String myAmqpVhost = "myAmqpHostname";
            backupPeer1Details.put(HOSTNAME, myAmqpVhost);

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(backupPeer1Details);

            Map<Symbol,Object> server1ConnectionProperties = new HashMap<Symbol, Object>();
            server1ConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(server1ConnectionProperties);
            primaryPeer.expectBegin();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected, now containing initial peer and the backup (with vhost details)
            List<URI> afterOpenFailoverURIs = new ArrayList<>();
            afterOpenFailoverURIs.add(primaryPeerURI);
            afterOpenFailoverURIs.add(new URI(backupPeerURI.toString() + "?amqp.vhost=" + myAmqpVhost));

            assertFailoverURIList(connection, afterOpenFailoverURIs);

            // Verify the client fails over to the advertised backup, and uses the correct AMQP hostname when doing so
            backupPeer.expectSaslAnonymous();
            backupPeer.expectOpen(null, Matchers.equalTo(myAmqpVhost), false);
            backupPeer.expectBegin();

            primaryPeer.close();

            backupPeer.waitForAllHandlersToComplete(3000);

            backupPeer.expectClose();
            connection.close();
            backupPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /*
     * Verify that when the Open frame contains a failover server list and we are connected via SSL configured with
     * system properties the redirect uses those properties to connect to the new host.
     */
    @Test(timeout = 20000)
    public void testFailoverUsingSSLConfiguredBySystemProperties() throws Exception {
        TransportOptions serverSslOptions = new TransportOptions();
        serverSslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverSslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverSslOptions.setKeyStorePassword(PASSWORD);
        serverSslOptions.setTrustStorePassword(PASSWORD);
        serverSslOptions.setVerifyHost(false);

        SSLContext serverSslContext = TransportSupport.createJdkSslContext(serverSslOptions);

        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);

        try (TestAmqpPeer primaryPeer = new TestAmqpPeer(serverSslContext, false);
             TestAmqpPeer backupPeer = new TestAmqpPeer(serverSslContext, false);) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            final URI backupPeerURI = createPeerURI(backupPeer);
            LOG.info("Primary is at: {}", primaryPeerURI);
            LOG.info("Backup is at: {}", backupPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);
            final CountDownLatch connectedToBackup = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is created
            primaryPeer.expectSaslAnonymous();

            // We only give it the primary/dropping peer details. It can only connect to the backup
            // peer by identifying the details in the announced failover-server-list.
            final JmsConnection connection = establishAnonymousConnecton(null, null, true, primaryPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                    if (isExpectedHost(backupPeerURI, remoteURI)) {
                        connectedToBackup.countDown();
                    }
                }
            });

            // Verify the existing failover URIs are as expected, the initial peer only
            List<URI> beforeOpenFailoverURIs = new ArrayList<>();
            beforeOpenFailoverURIs.add(primaryPeerURI);

            assertFailoverURIList(connection, beforeOpenFailoverURIs);

            // Set the primary up to expect the connection, have the failover list containing the backup advertised
            Map<Symbol,Object> backupPeerDetails = new HashMap<>();
            backupPeerDetails.put(NETWORK_HOST, "localhost");
            backupPeerDetails.put(PORT, backupPeer.getServerPort());

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(backupPeerDetails);

            Map<Symbol,Object> serverConnectionProperties = new HashMap<Symbol, Object>();
            serverConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(serverConnectionProperties);
            primaryPeer.expectBegin();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected, now containing initial peer and the backup1
            List<URI> afterOpenFailoverURIs = new ArrayList<>();
            afterOpenFailoverURIs.add(primaryPeerURI);
            afterOpenFailoverURIs.add(backupPeerURI);

            assertFailoverURIList(connection, afterOpenFailoverURIs);

            // Verify the client fails over to the advertised backup
            backupPeer.expectSaslAnonymous();
            backupPeer.expectOpen();
            backupPeer.expectBegin();
            backupPeer.expectBegin();

            // Create a predictable connection drop condition
            primaryPeer.expectBegin();
            primaryPeer.remotelyCloseConnection(true, AmqpError.INTERNAL_ERROR, "Remote is going down");

            connection.createSession();

            primaryPeer.waitForAllHandlersToComplete(1000);
            primaryPeer.close();

            assertTrue("Should connect to backup peer", connectedToBackup.await(5, TimeUnit.SECONDS));

            backupPeer.waitForAllHandlersToComplete(3000);

            backupPeer.expectClose();
            connection.close();
            backupPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /*
     * Verify that when the Open frame contains a failover server list and we are connected via SSL configured with
     * URI options on the AMQP URI the redirect uses those properties to connect to the new host.
     */
    @Test(timeout = 20000)
    public void testFailoverUsingSSLConfiguredByTransportOptions() throws Exception {
        TransportOptions sslOptions = new TransportOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext serverSslContext = TransportSupport.createJdkSslContext(sslOptions);

        try (TestAmqpPeer primaryPeer = new TestAmqpPeer(serverSslContext, false);
             TestAmqpPeer backupPeer = new TestAmqpPeer(serverSslContext, false);) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            final URI backupPeerURI = createPeerURI(backupPeer);
            LOG.info("Primary is at: {}", primaryPeerURI);
            LOG.info("Backup is at: {}", backupPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);
            final CountDownLatch connectedToBackup = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is
            // created
            primaryPeer.expectSaslAnonymous();

            String connectionOptions = "transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                                       "transport.trustStorePassword=" + PASSWORD;

            Map<String, String> expectedUriOptions = new LinkedHashMap<>();
            expectedUriOptions.put("transport.trustStoreLocation", CLIENT_JKS_TRUSTSTORE);
            expectedUriOptions.put("transport.trustStorePassword", PASSWORD);

            // We only give it the primary/dropping peer details. It can only
            // connect to the backup
            // peer by identifying the details in the announced
            // failover-server-list.
            final JmsConnection connection = establishAnonymousConnecton(connectionOptions, null, true, primaryPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                    if (isExpectedHost(backupPeerURI, remoteURI)) {
                        connectedToBackup.countDown();
                    }
                }
            });

            // Verify the existing failover URIs are as expected, the initial
            // peer only
            List<URI> beforeOpenFailoverURIs = new ArrayList<>();
            beforeOpenFailoverURIs.add(URISupport.applyParameters(primaryPeerURI, expectedUriOptions));

            assertFailoverURIList(connection, beforeOpenFailoverURIs);

            // Set the primary up to expect the connection, have the failover
            // list containing the backup advertised
            Map<Symbol, Object> backupPeerDetails = new HashMap<>();
            backupPeerDetails.put(NETWORK_HOST, "localhost");
            backupPeerDetails.put(PORT, backupPeer.getServerPort());

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(backupPeerDetails);

            Map<Symbol, Object> serverConnectionProperties = new HashMap<Symbol, Object>();
            serverConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(serverConnectionProperties);
            primaryPeer.expectBegin();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected, now containing initial
            // peer and the backup1
            List<URI> afterOpenFailoverURIs = new ArrayList<>();
            afterOpenFailoverURIs.add(URISupport.applyParameters(primaryPeerURI, expectedUriOptions));
            afterOpenFailoverURIs.add(URISupport.applyParameters(backupPeerURI, expectedUriOptions));

            assertFailoverURIList(connection, afterOpenFailoverURIs);

            // Verify the client fails over to the advertised backup
            backupPeer.expectSaslAnonymous();
            backupPeer.expectOpen();
            backupPeer.expectBegin();
            backupPeer.expectBegin();

            // Create a predictable connection drop condition
            primaryPeer.expectBegin();
            primaryPeer.remotelyCloseConnection(true, AmqpError.INTERNAL_ERROR, "Remote is going down");

            connection.createSession();

            primaryPeer.waitForAllHandlersToComplete(1000);
            primaryPeer.close();

            assertTrue("Should connect to backup peer", connectedToBackup.await(5, TimeUnit.SECONDS));

            backupPeer.waitForAllHandlersToComplete(3000);

            backupPeer.expectClose();
            connection.close();
            backupPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /*
     * Verify that when the Open frame contains a failover server list and we are connected via SSL
     * configured with the Failover URI with nested options the redirect uses those properties to
     * connect to the new host.
     */
    @Test(timeout = 20000)
    public void testFailoverUsingSSLConfiguredByNestedTransportOptions() throws Exception {
        TransportOptions sslOptions = new TransportOptions();
        sslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext serverSslContext = TransportSupport.createJdkSslContext(sslOptions);

        try (TestAmqpPeer primaryPeer = new TestAmqpPeer(serverSslContext, false);
             TestAmqpPeer backupPeer = new TestAmqpPeer(serverSslContext, false);) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            final URI backupPeerURI = createPeerURI(backupPeer);
            LOG.info("Primary is at: {}", primaryPeerURI);
            LOG.info("Backup is at: {}", backupPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);
            final CountDownLatch connectedToBackup = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is
            // created
            primaryPeer.expectSaslAnonymous();

            String failoverOptions = "?failover.nested.transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" +
                "failover.nested.transport.trustStorePassword=" + PASSWORD;

            Map<String, String> expectedUriOptions = new LinkedHashMap<>();
            expectedUriOptions.put("transport.trustStoreLocation", CLIENT_JKS_TRUSTSTORE);
            expectedUriOptions.put("transport.trustStorePassword", PASSWORD);

            // We only give it the primary/dropping peer details. It can only
            // connect to the backup
            // peer by identifying the details in the announced
            // failover-server-list.
            final JmsConnection connection = establishAnonymousConnecton(null, failoverOptions, true, primaryPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                    if (isExpectedHost(backupPeerURI, remoteURI)) {
                        connectedToBackup.countDown();
                    }
                }
            });

            // Verify the existing failover URIs are as expected, the initial
            // peer only
            List<URI> beforeOpenFailoverURIs = new ArrayList<>();
            beforeOpenFailoverURIs.add(URISupport.applyParameters(primaryPeerURI, expectedUriOptions));

            assertFailoverURIList(connection, beforeOpenFailoverURIs);

            // Set the primary up to expect the connection, have the failover
            // list containing the backup advertised
            Map<Symbol, Object> backupPeerDetails = new HashMap<>();
            backupPeerDetails.put(NETWORK_HOST, "localhost");
            backupPeerDetails.put(PORT, backupPeer.getServerPort());

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(backupPeerDetails);

            Map<Symbol, Object> serverConnectionProperties = new HashMap<Symbol, Object>();
            serverConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(serverConnectionProperties);
            primaryPeer.expectBegin();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected, now containing initial
            // peer and the backup1
            List<URI> afterOpenFailoverURIs = new ArrayList<>();
            afterOpenFailoverURIs.add(URISupport.applyParameters(primaryPeerURI, expectedUriOptions));
            afterOpenFailoverURIs.add(URISupport.applyParameters(backupPeerURI, expectedUriOptions));

            assertFailoverURIList(connection, afterOpenFailoverURIs);

            // Verify the client fails over to the advertised backup
            backupPeer.expectSaslAnonymous();
            backupPeer.expectOpen();
            backupPeer.expectBegin();
            backupPeer.expectBegin();

            // Create a predictable connection drop condition
            primaryPeer.expectBegin();
            primaryPeer.remotelyCloseConnection(true, AmqpError.INTERNAL_ERROR, "Remote is going down");

            connection.createSession();

            primaryPeer.waitForAllHandlersToComplete(1000);
            primaryPeer.close();

            assertTrue("Should connect to backup peer", connectedToBackup.await(5, TimeUnit.SECONDS));

            backupPeer.waitForAllHandlersToComplete(3000);

            backupPeer.expectClose();
            connection.close();
            backupPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /*
     * Verify that when the Open frame contains a failover server list and we are connected via SSL
     * configured with with a custom SSLContext the redirect uses those properties to connect to
     * the new host.
     */
    @Test(timeout = 20000)
    public void testFailoverUsingSSLConfiguredByCustomSSLContext() throws Exception {
        TransportOptions serverSslOptions = new TransportOptions();
        serverSslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverSslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverSslOptions.setKeyStorePassword(PASSWORD);
        serverSslOptions.setTrustStorePassword(PASSWORD);
        serverSslOptions.setVerifyHost(false);

        SSLContext serverSslContext = TransportSupport.createJdkSslContext(serverSslOptions);

        TransportOptions clientSslOptions = new TransportOptions();
        clientSslOptions.setKeyStoreLocation(CLIENT_JKS_KEYSTORE);
        clientSslOptions.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE);
        clientSslOptions.setKeyStorePassword(PASSWORD);
        clientSslOptions.setTrustStorePassword(PASSWORD);

        SSLContext clientSslContext = TransportSupport.createJdkSslContext(clientSslOptions);

        try (TestAmqpPeer primaryPeer = new TestAmqpPeer(serverSslContext, false);
             TestAmqpPeer backupPeer = new TestAmqpPeer(serverSslContext, false);) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            final URI backupPeerURI = createPeerURI(backupPeer);
            LOG.info("Primary is at: {}", primaryPeerURI);
            LOG.info("Backup is at: {}", backupPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);
            final CountDownLatch connectedToBackup = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is
            // created
            primaryPeer.expectSaslAnonymous();

            // We only give it the primary/dropping peer details. It can only
            // connect to the backup peer by identifying the details in the announced
            // failover-server-list.
            final JmsConnectionFactory factory = new JmsConnectionFactory(
                    "failover:(amqps://localhost:" + primaryPeer.getServerPort() + ")");
            factory.setSslContext(clientSslContext);

            JmsConnection connection = (JmsConnection) factory.createConnection();
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                    if (isExpectedHost(backupPeerURI, remoteURI)) {
                        connectedToBackup.countDown();
                    }
                }
            });

            // Verify the existing failover URIs are as expected, the initial peer only
            List<URI> beforeOpenFailoverURIs = new ArrayList<>();
            beforeOpenFailoverURIs.add(primaryPeerURI);

            assertFailoverURIList(connection, beforeOpenFailoverURIs);

            // Set the primary up to expect the connection, have the failover
            // list containing the backup advertised
            Map<Symbol, Object> backupPeerDetails = new HashMap<>();
            backupPeerDetails.put(NETWORK_HOST, "localhost");
            backupPeerDetails.put(PORT, backupPeer.getServerPort());

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(backupPeerDetails);

            Map<Symbol, Object> serverConnectionProperties = new HashMap<Symbol, Object>();
            serverConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(serverConnectionProperties);
            primaryPeer.expectBegin();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected, now containing initial
            // peer and the backup1
            List<URI> afterOpenFailoverURIs = new ArrayList<>();
            afterOpenFailoverURIs.add(primaryPeerURI);
            afterOpenFailoverURIs.add(backupPeerURI);

            assertFailoverURIList(connection, afterOpenFailoverURIs);

            // Verify the client fails over to the advertised backup
            backupPeer.expectSaslAnonymous();
            backupPeer.expectOpen();
            backupPeer.expectBegin();
            backupPeer.expectBegin();

            // Create a predictable connection drop condition
            primaryPeer.expectBegin();
            primaryPeer.remotelyCloseConnection(true, AmqpError.INTERNAL_ERROR, "Remote is going down");

            connection.createSession();

            primaryPeer.waitForAllHandlersToComplete(1000);
            primaryPeer.close();

            assertTrue("Should connect to backup peer", connectedToBackup.await(5, TimeUnit.SECONDS));

            backupPeer.waitForAllHandlersToComplete(3000);

            backupPeer.expectClose();
            connection.close();
            backupPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /*
     * Verify that when the Open frame contains a failover server list and we are connected via SSL
     * that a remote listed in the open frame failover list is ignored when insecure redirects are
     * prohibited.
     */
    @Test(timeout = 20000)
    public void testFailoverIgnoresInsecureServerWhenNotConfiguredToAllow() throws Exception {
        doTestFailoverHandlingOfInsecureRedirectAdvertisement(false);
    }

    /*
     * Verify that when the Open frame contains a failover server list and we are connected via SSL
     * that a remote listed in the open frame failover list is accepted when insecure redirects are
     * allowed.
     */
    @Test(timeout = 20000)
    public void testFailoverAcceptsInsecureServerWhenConfiguredToAllow() throws Exception {
        doTestFailoverHandlingOfInsecureRedirectAdvertisement(true);
    }

    private void doTestFailoverHandlingOfInsecureRedirectAdvertisement(boolean allow) throws Exception {

        TransportOptions serverSslOptions = new TransportOptions();
        serverSslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverSslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverSslOptions.setKeyStorePassword(PASSWORD);
        serverSslOptions.setTrustStorePassword(PASSWORD);
        serverSslOptions.setVerifyHost(false);

        SSLContext serverSslContext = TransportSupport.createJdkSslContext(serverSslOptions);

        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);

        try (TestAmqpPeer primaryPeer = new TestAmqpPeer(serverSslContext, false)) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            LOG.info("Primary is at: {}", primaryPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is created
            primaryPeer.expectSaslAnonymous();

            String failoverOptions = "failover.nested.amqp.allowNonSecureRedirects=" + allow;
            String connectionOptions = "amqp.allowNonSecureRedirects=" + allow;

            // We only give it the primary/dropping peer details. It can only connect to the backup
            // peer by identifying the details in the announced failover-server-list.
            final JmsConnection connection = establishAnonymousConnecton(null, failoverOptions, true, primaryPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                }
            });

            // Verify the existing failover URIs are as expected, the initial peer only
            List<URI> beforeOpenFailoverURIs = new ArrayList<>();
            beforeOpenFailoverURIs.add(new URI(primaryPeerURI + "?" + connectionOptions));

            assertFailoverURIList(connection, beforeOpenFailoverURIs);

            // Set the primary up to expect the connection, have the failover list containing the backup advertised
            Map<Symbol,Object> backupPeerDetails = new HashMap<>();
            backupPeerDetails.put(NETWORK_HOST, "localhost");
            backupPeerDetails.put(PORT, 5673);
            backupPeerDetails.put(SCHEME, "amqp");

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(backupPeerDetails);

            Map<Symbol,Object> serverConnectionProperties = new HashMap<Symbol, Object>();
            serverConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(serverConnectionProperties);
            primaryPeer.expectBegin();
            primaryPeer.expectClose();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected, now containing initial peer and if non-secure redirect
            // was permitted, the non-secure backup as well.
            List<URI> afterOpenFailoverURIs = new ArrayList<>();
            afterOpenFailoverURIs.add(new URI(primaryPeerURI + "?" + connectionOptions));
            if (allow) {
                afterOpenFailoverURIs.add(new URI("amqp://localhost:5673?" + connectionOptions));
            }

            assertFailoverURIList(connection, afterOpenFailoverURIs);

            connection.close();

            primaryPeer.waitForAllHandlersToComplete(1000);
        }
    }

    /*
     * Verify that when the Open frame contains a failover server list and we are connected via
     * the 'amqp' transport and the redirect contains a 'ws' scheme that failover reconnect list
     * is updated to contain the 'amqpws' redirect.
     */
    @Test(timeout = 20000)
    public void testFailoverAcceptsUpdateUsingTransportSchemeWS() throws Exception {
        doTestFailoverAcceptsUpdateUsingTransportSchemes("ws", "amqpws");
    }

    /*
     * Verify that when the Open frame contains a failover server list and we are connected via
     * the 'amqp' transport and the redirect contains a 'wss' scheme that failover reconnect list
     * is updated to contain the 'amqpwss' redirect.
     */
    @Test(timeout = 20000)
    public void testFailoverAcceptsUpdateUsingTransportSchemeWSS() throws Exception {
        doTestFailoverAcceptsUpdateUsingTransportSchemes("wss", "amqpwss");
    }

    private void doTestFailoverAcceptsUpdateUsingTransportSchemes(String transportScheme, String expected) throws Exception {

        TransportOptions serverSslOptions = new TransportOptions();
        serverSslOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverSslOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverSslOptions.setKeyStorePassword(PASSWORD);
        serverSslOptions.setTrustStorePassword(PASSWORD);
        serverSslOptions.setVerifyHost(false);

        SSLContext serverSslContext = TransportSupport.createJdkSslContext(serverSslOptions);

        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);

        try (TestAmqpPeer primaryPeer = new TestAmqpPeer(serverSslContext, false)) {

            final URI primaryPeerURI = createPeerURI(primaryPeer);
            LOG.info("Primary is at: {}", primaryPeerURI);

            final CountDownLatch connectedToPrimary = new CountDownLatch(1);

            // Expect the authentication as soon as the connection object is created
            primaryPeer.expectSaslAnonymous();

            // Allow non-secure redirects for this test for simplicity.
            String failoverOptions = "failover.nested.amqp.allowNonSecureRedirects=true";
            String connectionOptions = "amqp.allowNonSecureRedirects=true";

            // We only give it the primary/dropping peer details. It can only connect to the backup
            // peer by identifying the details in the announced failover-server-list.
            final JmsConnection connection = establishAnonymousConnecton(null, failoverOptions, true, primaryPeer);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (isExpectedHost(primaryPeerURI, remoteURI)) {
                        connectedToPrimary.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Reestablished: {}", remoteURI);
                }
            });

            // Verify the existing failover URIs are as expected, the initial peer only
            List<URI> beforeOpenFailoverURIs = new ArrayList<>();
            beforeOpenFailoverURIs.add(new URI(primaryPeerURI + "?" + connectionOptions));

            assertFailoverURIList(connection, beforeOpenFailoverURIs);

            // Set the primary up to expect the connection, have the failover list containing the backup advertised
            Map<Symbol,Object> backupPeerDetails = new HashMap<>();
            backupPeerDetails.put(NETWORK_HOST, "localhost");
            backupPeerDetails.put(PORT, 5673);
            backupPeerDetails.put(SCHEME, transportScheme);

            List<Map<Symbol, Object>> failoverServerList = new ArrayList<Map<Symbol, Object>>();
            failoverServerList.add(backupPeerDetails);

            Map<Symbol,Object> serverConnectionProperties = new HashMap<Symbol, Object>();
            serverConnectionProperties.put(FAILOVER_SERVER_LIST, failoverServerList);

            primaryPeer.expectOpen(serverConnectionProperties);
            primaryPeer.expectBegin();
            primaryPeer.expectClose();

            // Provoke the actual AMQP connection
            connection.start();

            assertTrue("Should connect to primary peer", connectedToPrimary.await(5, TimeUnit.SECONDS));

            // Verify the failover URIs are as expected, containing initial peer and a backup with expected uri scheme.
            List<URI> afterOpenFailoverURIs = new ArrayList<>();
            afterOpenFailoverURIs.add(new URI(primaryPeerURI + "?" + connectionOptions));
            afterOpenFailoverURIs.add(new URI(expected + "://localhost:5673?" + connectionOptions));

            assertFailoverURIList(connection, afterOpenFailoverURIs);

            connection.close();

            primaryPeer.waitForAllHandlersToComplete(1000);
        }
    }

    private void setSslSystemPropertiesForCurrentTest(String keystore, String keystorePassword, String truststore, String truststorePassword) {
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE, keystore);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE, truststore);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
    }

    private void assertFailoverURIList(JmsConnection connection, List<URI> expectedURIs) throws Exception {
        FailoverProvider provider = getFailoverProvider(connection);

        Field urisField = provider.getClass().getDeclaredField("uris");
        urisField.setAccessible(true);
        Object urisObj = urisField.get(provider);

        assertNotNull("Expected to get a uri pool instance", urisObj);
        assertTrue("Unexpected uri pool type: " + urisObj.getClass(), urisObj instanceof FailoverUriPool);
        FailoverUriPool uriPool = (FailoverUriPool) urisObj;

        List<URI> current = uriPool.getList();
        assertEquals(expectedURIs, current);
    }

    private FailoverProvider getFailoverProvider(JmsConnection connection) throws Exception {
        Field field = connection.getClass().getDeclaredField("provider");
        field.setAccessible(true);
        Object providerObj = field.get(connection);

        assertNotNull("Expected to get a provdier instance", providerObj);
        assertTrue("Unexpected provider type: " + providerObj.getClass(), providerObj instanceof FailoverProvider);
        FailoverProvider provider = (FailoverProvider) providerObj;
        return provider;
    }

    private JmsConnection establishAnonymousConnecton(String failoverParams, TestAmqpPeer... peers) throws Exception {
        return establishAnonymousConnecton(null, failoverParams, peers);
    }

    private JmsConnection establishAnonymousConnecton(String connectionParams, String failoverParams, TestAmqpPeer... peers) throws Exception {
        return establishAnonymousConnecton(connectionParams, failoverParams, false, peers);
    }

    private JmsConnection establishAnonymousConnecton(String connectionParams, String failoverParams, boolean ssl, TestAmqpPeer... peers) throws Exception {
        if (peers.length == 0) {
            throw new IllegalArgumentException("No test peers were given, at least 1 required");
        }

        String remoteURI = "failover:(";
        boolean first = true;
        for (TestAmqpPeer peer : peers) {
            if (!first) {
                remoteURI += ",";
            }
            remoteURI += createPeerURI(peer, connectionParams).toString();
            first = false;
        }

        if (failoverParams == null) {
            remoteURI += ")?failover.maxReconnectAttempts=10";
        } else {
            remoteURI += ")" + (failoverParams.startsWith("?") ? "" : "?") + failoverParams;
        }

        ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
        Connection connection = factory.createConnection();

        return (JmsConnection) connection;
    }

    private URI createPeerURI(TestAmqpPeer peer) throws Exception {
        return createPeerURI(peer, null);
    }

    private URI createPeerURI(TestAmqpPeer peer, String params) throws Exception {
        String scheme = peer.isSSL() ? "amqps" : "amqp";
        URI result = new URI(scheme, "localhost:" + peer.getServerPort(), null, null, null);

        Map<String, String> queryParameters = PropertyUtil.parseQuery(params);

        return URISupport.applyParameters(result, queryParameters);
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
}