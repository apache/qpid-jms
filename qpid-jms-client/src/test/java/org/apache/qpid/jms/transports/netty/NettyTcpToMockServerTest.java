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
package org.apache.qpid.jms.transports.netty;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.NETWORK_HOST;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.OPEN_HOSTNAME;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PATH;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PORT;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SCHEME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.InvalidClientIDException;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionExtensions;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.netty.NettySimpleAmqpServer.ConnectionIntercepter;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ConnectionError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete;

/**
 * Test Client connection to Mock AMQP server.
 */
public class NettyTcpToMockServerTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTcpToMockServerTest.class);

    @Rule
    public TestName test = new TestName();

    @Test(timeout = 60 * 1000)
    public void testConnectToServer() throws Exception {
        try (NettySimpleAmqpServer server = createServer(createServerOptions())) {
            server.start();

            JmsConnectionFactory cf = new JmsConnectionFactory(createConnectionURI(server));
            Connection connection = null;
            try {
                connection = cf.createConnection();
                connection.start();
            } catch (Exception ex) {
                LOG.error("Caught exception while attempting to connect");
                fail("Should be able to connect in this simple test");
            } finally {
                connection.close();
            }
        }
    }

    @Test(timeout = 60 * 1000)
    public void testServerEnforcesExclusiveContainerCapability() throws Exception {
        try (NettySimpleAmqpServer server = createServer(createServerOptions())) {
            server.setAllowNonSaslConnections(true);
            server.start();

            JmsConnectionFactory cf = new JmsConnectionFactory(createConnectionURI(server));
            Connection connection = null;
            try {
                connection = cf.createConnection();
                connection.setClientID(test.getMethodName());
            } catch (Exception ex) {
                LOG.info("Caught exception while attempting to connect");
                fail("Should not throw an exception for this connection");
            }

            try {
                cf.createConnection().setClientID(test.getMethodName());
                fail("Should throw an exception when for duplicate client ID");
            } catch (InvalidClientIDException ex) {
                LOG.info("Caught exception while attempting to connect");
            } catch (Exception ex) {
                LOG.info("Caught exception while attempting to connect");
                fail("Should throw an InvalidClientIDException when for duplicate client ID");
            } finally {
                connection.close();
            }
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerFailsWhenSaslDisabled() throws Exception {
        try (NettySimpleAmqpServer server = createServer(createServerOptions())) {
            server.start();

            JmsConnectionFactory cf = new JmsConnectionFactory(createConnectionURI(server, "?amqp.saslLayer=false"));
            Connection connection = null;
            try {
                connection = cf.createConnection();
                connection.start();
                fail("Should throw an exception when not using SASL");
            } catch (Exception ex) {
                LOG.info("Caught expected exception while attempting to connect");
            } finally {
                connection.close();
            }
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWhenSaslDisabledAndServerAllowsIt() throws Exception {
        try (NettySimpleAmqpServer server = createServer(createServerOptions())) {
            server.setAllowNonSaslConnections(true);
            server.start();

            JmsConnectionFactory cf = new JmsConnectionFactory(createConnectionURI(server, "?amqp.saslLayer=false"));
            Connection connection = null;
            try {
                connection = cf.createConnection();
                connection.start();
            } catch (Exception ex) {
                LOG.info("Caught exception while attempting to connect");
                fail("Should not throw an exception when not using SASL");
            } finally {
                connection.close();
            }
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWhenRedirected() throws Exception {
        try (NettySimpleAmqpServer server = createServer(createServerOptions());
             NettySimpleAmqpServer redirect = createServer(createServerOptions())) {

            final CountDownLatch redirectComplete = new CountDownLatch(1);

            server.setConnectionIntercepter(new ConnectionIntercepter() {

                @Override
                public ErrorCondition interceptConnectionAttempt(org.apache.qpid.proton.engine.Connection connection) {
                    ErrorCondition redirection = new ErrorCondition(ConnectionError.REDIRECT,
                        "Server redirecting connection.");

                    URI serverURI = null;
                    try {
                        serverURI = redirect.getConnectionURI();
                    } catch (Exception e) {
                        new RuntimeException();
                    }

                    // Create standard redirection condition
                    Map<Symbol, Object> infoMap = new HashMap<Symbol, Object> ();
                    infoMap.put(OPEN_HOSTNAME, serverURI.getHost());
                    infoMap.put(NETWORK_HOST, serverURI.getHost());
                    infoMap.put(PORT, serverURI.getPort());
                    redirection.setInfo(infoMap);

                    return redirection;
                }
            });

            redirect.setConnectionIntercepter(new ConnectionIntercepter() {

                @Override
                public ErrorCondition interceptConnectionAttempt(org.apache.qpid.proton.engine.Connection connection) {
                    redirectComplete.countDown();
                    return null;
                }
            });

            server.start();
            redirect.start();

            JmsConnectionFactory cf = new JmsConnectionFactory(createFailoverURI(server));
            Connection connection = null;
            try {
                connection = cf.createConnection();
                connection.start();
            } catch (Exception ex) {
                LOG.error("Caught exception while attempting to connect");
                connection.close();
                fail("Should be able to connect to the redirect server");
            }

            server.stop();

            try {
                // We should be connected to the redirect server so this should
                // work even though the initial server is shutdown.
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            } catch (Exception ex) {
                LOG.error("Caught exception while attempting to connect");
                fail("Should be able to connect to the redirect server");
            } finally {
                connection.close();
            }

            assertTrue(redirectComplete.await(10, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 20 * 1000)
    public void testConnectToWSServerWhenRedirectedWithNewPath() throws Exception {
        try (NettySimpleAmqpServer primary = createWSServer(createServerOptions());
             NettySimpleAmqpServer redirect = createWSServer(createServerOptions())) {

            final CountDownLatch redirectComplete = new CountDownLatch(1);

            primary.setWebSocketPath("/primary");
            redirect.setWebSocketPath("/redirect");

            primary.setConnectionIntercepter(new ConnectionIntercepter() {

                @Override
                public ErrorCondition interceptConnectionAttempt(org.apache.qpid.proton.engine.Connection connection) {
                    ErrorCondition redirection = new ErrorCondition(ConnectionError.REDIRECT,
                        "Server redirecting connection.");

                    URI serverURI = null;
                    try {
                        serverURI = redirect.getConnectionURI();
                    } catch (Exception e) {
                        new RuntimeException();
                    }

                    // Create standard redirection condition
                    Map<Symbol, Object> infoMap = new HashMap<Symbol, Object> ();
                    infoMap.put(OPEN_HOSTNAME, serverURI.getHost());
                    infoMap.put(NETWORK_HOST, serverURI.getHost());
                    infoMap.put(PORT, serverURI.getPort());
                    infoMap.put(PATH, redirect.getWebSocketPath());
                    infoMap.put(SCHEME, redirect.isSecureServer() ? "wss" : "ws");
                    redirection.setInfo(infoMap);

                    return redirection;
                }
            });

            redirect.setConnectionIntercepter(new ConnectionIntercepter() {

                @Override
                public ErrorCondition interceptConnectionAttempt(org.apache.qpid.proton.engine.Connection connection) {
                    redirectComplete.countDown();
                    return null;
                }
            });

            primary.start();
            redirect.start();

            JmsConnectionFactory cf = new JmsConnectionFactory(createFailoverURI(primary));
            Connection connection = null;
            try {
                connection = cf.createConnection();
                connection.start();
            } catch (Exception ex) {
                LOG.error("Caught exception while attempting to connect");
                connection.close();
                fail("Should be able to connect to the redirect server");
            }

            primary.stop();

            try {
                // We should be connected to the redirect server so this should
                // work even though the initial server is shutdown.
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            } catch (Exception ex) {
                LOG.error("Caught exception while attempting to connect");
                fail("Should be able to connect to the redirect server");
            } finally {
                connection.close();
            }

            assertTrue(redirectComplete.await(10, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 60000)
    public void testConnectToWSServerWithHttpHeaderConnectionExtensions() throws Exception {
        try (NettySimpleAmqpServer server = createWSServer(createServerOptions())) {
            server.start();

            AtomicReference<URI> remoteURI = new AtomicReference<URI>();

            JmsConnectionFactory cf = new JmsConnectionFactory(createConnectionURI(server));
            cf.setExtension(JmsConnectionExtensions.HTTP_HEADERS_OVERRIDE.toString(), (connection, uri) -> {
                remoteURI.set(uri);

                Map<String, String> headers = new HashMap<>();

                headers.put("test-header1", "FOO");
                headers.put("test-header2", "BAR");

                return headers;
            });

            Connection connection = null;
            try {
                connection = cf.createConnection();
                connection.start();

                assertTrue("HandshakeCompletion not set within given time", server.awaitHandshakeCompletion(2000));
                HandshakeComplete handshake = server.getHandshakeComplete();
                assertNotNull("completion should not be null", handshake);
                HttpHeaders requestHeaders = handshake.requestHeaders();

                assertTrue(requestHeaders.contains("test-header1"));
                assertTrue(requestHeaders.contains("test-header2"));

                assertEquals("FOO", requestHeaders.get("test-header1"));
                assertEquals("BAR", requestHeaders.get("test-header2"));

                assertNotNull(remoteURI.get());
                assertEquals(server.getConnectionURI(), remoteURI.get());
            } catch (Exception ex) {
                LOG.error("Caught exception while attempting to connect");
                fail("Should be able to connect in this simple test");
            } finally {
                connection.close();
            }
        }
    }

    protected URI createConnectionURI(NettyServer server) throws Exception {
        return createConnectionURI(server, null);
    }

    protected URI createConnectionURI(NettyServer server, String options) throws Exception {
        String serverLocation = server.getConnectionURI().toString();
        if (options != null && !options.isEmpty()) {
            if (options.startsWith("?")) {
                serverLocation += options;
            } else {
                serverLocation = serverLocation + "?" + options;
            }
        }

        return new URI(serverLocation);
    }

    protected URI createFailoverURI(NettyServer server) throws Exception {
        URI serverURI = createConnectionURI(server, null);

        String failoverURI = "failover:(" + serverURI.toString() + "?amqp.vhost=localhost)?failover.maxReconnectAttempts=3";

        return new URI(failoverURI);
    }

    protected TransportOptions createServerOptions() {
        return new TransportOptions();
    }

    protected NettySimpleAmqpServer createServer(TransportOptions options) {
        return new NettySimpleAmqpServer(options, false);
    }

    protected NettySimpleAmqpServer createServer(TransportOptions options, boolean needClientAuth) {
        return new NettySimpleAmqpServer(options, false, needClientAuth);
    }

    protected NettySimpleAmqpServer createWSServer(TransportOptions options) {
        return new NettySimpleAmqpServer(options, false, false, true);
    }
}
