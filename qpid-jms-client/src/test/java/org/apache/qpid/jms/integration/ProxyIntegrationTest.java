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
package org.apache.qpid.jms.integration;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionExtensions;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.proxy.TestProxy;
import org.apache.qpid.jms.test.proxy.TestProxy.ProxyType;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.apache.qpid.jms.transports.netty.NettySimpleAmqpServer;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.Repeat;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;

@RunWith(QpidJMSTestRunner.class)
public class ProxyIntegrationTest extends QpidJmsTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(ProxyIntegrationTest.class);
    private static final String BROKER_PKCS12_KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String PASSWORD = "password";

    @Test(timeout = 20000)
    public void testCreateConnectionViaSocksProxy() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();
             TestProxy testProxy = new TestProxy(ProxyType.SOCKS5)) {
            testProxy.start();

            AtomicInteger supplierUsageCount = new AtomicInteger();
            Supplier<ProxyHandler> proxyHandlerSupplier = () -> {
                supplierUsageCount.incrementAndGet();
                return new Socks5ProxyHandler(new InetSocketAddress("localhost", testProxy.getPort()));
            };

            Connection connection = establishConnecton(testPeer, proxyHandlerSupplier, false, null);

            testPeer.expectClose();
            connection.close();

            assertEquals(1, testProxy.getSuccessCount());
            assertEquals("Unexpected handler supplier usage count", 1, supplierUsageCount.get());
        }
    }

    @Test(timeout = 20000)
    public void testCreateSecureConnectionViaSocksProxy() throws Exception {
        TransportOptions sslOptions = new TransportOptions();
        sslOptions.setKeyStoreLocation(BROKER_PKCS12_KEYSTORE);
        sslOptions.setKeyStorePassword(PASSWORD);
        sslOptions.setVerifyHost(false);

        SSLContext context = TransportSupport.createJdkSslContext(sslOptions);
        try (TestAmqpPeer testPeer = new TestAmqpPeer(context, false);
             TestProxy testProxy = new TestProxy(ProxyType.SOCKS5)) {
            testProxy.start();

            String connOptions = "?transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE + "&" + "transport.trustStorePassword=" + PASSWORD
                    + "&" + "transport.useOpenSSL=" + false;
            AtomicInteger supplierUsageCount = new AtomicInteger();
            Supplier<ProxyHandler> proxyHandlerSupplier = () -> {
                supplierUsageCount.incrementAndGet();
                return new Socks5ProxyHandler(new InetSocketAddress("localhost", testProxy.getPort()));
            };

            Connection connection = establishConnecton(testPeer, proxyHandlerSupplier, true, connOptions);

            Socket socket = testPeer.getClientSocket();
            assertTrue(socket instanceof SSLSocket);

            testPeer.expectClose();
            connection.close();

            assertEquals(1, testProxy.getSuccessCount());
            assertEquals("Unexpected handler supplier usage count", 1, supplierUsageCount.get());
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testFailoverCreateConsumerAfterConnectionDropsViaSocksProxy() throws Exception {
        try (TestAmqpPeer originalPeer = new TestAmqpPeer();
             TestAmqpPeer finalPeer = new TestAmqpPeer();
             TestProxy testProxy = new TestProxy(ProxyType.SOCKS5)) {
            testProxy.start();

            int proxyPort = testProxy.getPort();
            final CountDownLatch originalConnected = new CountDownLatch(1);
            final CountDownLatch finalConnected = new CountDownLatch(1);

            // Create a peer to connect to, then one to reconnect to
            final String originalURI = createPeerURI(originalPeer);
            final String finalURI = createPeerURI(finalPeer);

            LOG.info("Original peer is at: {}", originalURI);
            LOG.info("Final peer is at: {}", finalURI);
            LOG.info("Proxy is at port: {}", proxyPort);

            // Connect to the first peer
            originalPeer.expectSaslAnonymous();
            originalPeer.expectOpen();
            originalPeer.expectBegin();
            originalPeer.expectBegin();
            originalPeer.dropAfterLastHandler();

            AtomicInteger supplierUsageCount = new AtomicInteger();
            Supplier<ProxyHandler> proxyHandlerSupplier = () -> {
                supplierUsageCount.incrementAndGet();
                return new Socks5ProxyHandler(new InetSocketAddress("localhost", proxyPort));
            };

            final JmsConnection connection = establishFailoverConnecton(proxyHandlerSupplier, originalPeer, finalPeer);
            ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setQueuePrefetch(0);
            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    LOG.info("Connection Established: {}", remoteURI);
                    if (originalURI.equals(remoteURI.toString())) {
                        originalConnected.countDown();
                    }
                }

                @Override
                public void onConnectionRestored(URI remoteURI) {
                    LOG.info("Connection Restored: {}", remoteURI);
                    if (finalURI.equals(remoteURI.toString())) {
                        finalConnected.countDown();
                    }
                }
            });
            connection.start();

            assertTrue("Should connect to original peer", originalConnected.await(5, TimeUnit.SECONDS));
            assertEquals("Unexpected handler supplier usage count", 1, supplierUsageCount.get());

            // --- Post Failover Expectations of FinalPeer --- //

            finalPeer.expectSaslAnonymous();
            finalPeer.expectOpen();
            finalPeer.expectBegin();
            finalPeer.expectBegin();
            finalPeer.expectReceiverAttach();
            finalPeer.expectLinkFlow(false, false, equalTo(UnsignedInteger.valueOf(1)));
            finalPeer.expectLinkFlow(true, true, equalTo(UnsignedInteger.valueOf(1)));
            finalPeer.expectDetach(true, true, true);
            finalPeer.expectClose();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            assertNull(consumer.receive(500));
            LOG.info("Receive returned");

            assertTrue("Should connect to final peer", finalConnected.await(5, TimeUnit.SECONDS));

            LOG.info("Closing consumer");
            consumer.close();

            // Shut it down
            connection.close();

            finalPeer.waitForAllHandlersToComplete(1000);

            // connection to originalPeer and finalPeer
            assertEquals(2, testProxy.getSuccessCount());
            assertEquals("Unexpected handler supplier usage count", 2, supplierUsageCount.get());
        }
    }

    @Test(timeout = 30000)
    public void testCreateWebSocketConnectionViaHttpProxyAndStart() throws Exception {
        doTestCreateWebSocketConnectionViaHttpProxyAndStart(false);
    }

    @Test(timeout = 30000)
    public void testCreateSecureWebSocketConnectionViaHttpProxyAndStart() throws Exception {
        doTestCreateWebSocketConnectionViaHttpProxyAndStart(true);
    }

    private void doTestCreateWebSocketConnectionViaHttpProxyAndStart(boolean secure) throws Exception {
        TransportOptions options = new TransportOptions();
        options.setKeyStoreLocation(BROKER_PKCS12_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setVerifyHost(false);

        final AtomicBoolean connectedThroughProxy = new AtomicBoolean();

        String connOptions = "?transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE +
                             "&transport.trustStorePassword=" + PASSWORD +
                             "&transport.useOpenSSL=" + false;

        try (NettySimpleAmqpServer server = new NettySimpleAmqpServer(options, secure, false, true);
             TestProxy testProxy = new TestProxy(ProxyType.HTTP)) {

            server.setConnectionIntercepter((protonConnection) -> {
                connectedThroughProxy.set(true);
                return null;
            });

            server.start();
            testProxy.start();

            JmsConnectionFactory factory = new JmsConnectionFactory(server.getConnectionURI() + connOptions);
            factory.setExtension(JmsConnectionExtensions.PROXY_HANDLER_SUPPLIER.toString(), (connection, remote) -> {
                SocketAddress proxyAddress = new InetSocketAddress("localhost", testProxy.getPort());
                Supplier<ProxyHandler> proxyHandlerFactory = () -> {
                    return new HttpProxyHandler(proxyAddress);
                };
                return proxyHandlerFactory;
            });

            Connection connection = factory.createConnection();
            assertNotNull(connection);

            connection.start();

            Session session = connection.createSession();
            assertNotNull(connection);
            session.close();

            connection.close();

            assertEquals(1, testProxy.getSuccessCount());
            assertTrue("Client did not connect to test server through the proxy.", connectedThroughProxy.get());
        }
    }

    @Test(timeout = 30000)
    public void testCreateWebSocketConnectionViaSocksProxyAndStart() throws Exception {
        doTestCreateWebSocketConnectionViaSocksProxyAndStart(false);
    }

    @Test(timeout = 30000)
    public void testCreateSecureWebSocketConnectionViaSocksProxyAndStart() throws Exception {
        doTestCreateWebSocketConnectionViaSocksProxyAndStart(true);
    }

    private void doTestCreateWebSocketConnectionViaSocksProxyAndStart(boolean secure) throws Exception {
        TransportOptions serverOptions = new TransportOptions();
        serverOptions.setKeyStoreLocation(BROKER_PKCS12_KEYSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);

        final AtomicBoolean connectedThroughProxy = new AtomicBoolean();

        String connOptions = "?transport.trustStoreLocation=" + CLIENT_JKS_TRUSTSTORE +
                             "&transport.trustStorePassword=" + PASSWORD +
                             "&transport.useOpenSSL=" + false;

        try (NettySimpleAmqpServer server = new NettySimpleAmqpServer(serverOptions, secure, false, true);
             TestProxy testProxy = new TestProxy(ProxyType.SOCKS5)) {

            server.setConnectionIntercepter((protonConnection) -> {
                connectedThroughProxy.set(true);
                return null;
            });

            server.start();
            testProxy.start();

            JmsConnectionFactory factory = new JmsConnectionFactory(server.getConnectionURI() + connOptions);
            factory.setExtension(JmsConnectionExtensions.PROXY_HANDLER_SUPPLIER.toString(), (connection, remote) -> {
                SocketAddress proxyAddress = new InetSocketAddress("localhost", testProxy.getPort());
                Supplier<ProxyHandler> proxyHandlerFactory = () -> {
                    return new Socks5ProxyHandler(proxyAddress);
                };
                return proxyHandlerFactory;
            });

            Connection connection = factory.createConnection();
            assertNotNull(connection);

            connection.start();

            Session session = connection.createSession();
            assertNotNull(connection);
            session.close();

            connection.close();

            assertEquals(1, testProxy.getSuccessCount());
            assertTrue("Client did not connect to test server through the proxy.", connectedThroughProxy.get());
        }
    }

    private Connection establishConnecton(TestAmqpPeer testPeer, Supplier<ProxyHandler> proxyHandlerSupplier, boolean ssl, String optionsString) throws JMSException {
        testPeer.expectSaslPlain("guest", "guest");
        testPeer.expectOpen();

        // Each connection creates a session for managing temporary destinations etc
        testPeer.expectBegin();

        String remoteURI = buildURI(testPeer, ssl, optionsString);
        LOG.debug("connect to {}", remoteURI);
        JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);
        factory.setExtension(JmsConnectionExtensions.PROXY_HANDLER_SUPPLIER.toString(), (connection1, remote) -> {
            return proxyHandlerSupplier;
        });
        Connection connection = factory.createConnection("guest", "guest");

        // Set a clientId to provoke the actual AMQP connection process to occur.
        connection.setClientID("clientName");

        assertNull(testPeer.getThrowable());

        return connection;
    }

    private JmsConnection establishFailoverConnecton(Supplier<ProxyHandler> proxyHandlerSupplier, TestAmqpPeer... peers) throws JMSException {
        if (peers.length == 0) {
            throw new IllegalArgumentException("No test peers were given, at least 1 required");
        }

        String remoteURI = "failover:(";
        boolean first = true;
        for (TestAmqpPeer peer : peers) {
            if (!first) {
                remoteURI += ",";
            }
            remoteURI += createPeerURI(peer, null);
            first = false;
        }

        remoteURI += ")?failover.maxReconnectAttempts=10";

        JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);
        factory.setExtension(JmsConnectionExtensions.PROXY_HANDLER_SUPPLIER.toString(), (connection, remote) -> {
            return proxyHandlerSupplier;
        });

        Connection connection = factory.createConnection();

        return (JmsConnection) connection;
    }

    private String createPeerURI(TestAmqpPeer peer) {
        return createPeerURI(peer, null);
    }

    private String createPeerURI(TestAmqpPeer peer, String params) {
        return "amqp://localhost:" + peer.getServerPort() + (params != null ? "?" + params : "");
    }

    private String buildURI(TestAmqpPeer testPeer, boolean ssl, String optionsString) {
        String scheme = ssl ? "amqps" : "amqp";
        final String baseURI = scheme + "://localhost:" + testPeer.getServerPort();
        String remoteURI = baseURI;
        if (optionsString != null) {
            if (optionsString.startsWith("?")) {
                remoteURI = baseURI + optionsString;
            } else {
                remoteURI = baseURI + "?" + optionsString;
            }
        }
        return remoteURI;
    }
}
