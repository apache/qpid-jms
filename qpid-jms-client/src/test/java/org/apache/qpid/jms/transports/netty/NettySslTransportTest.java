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
package org.apache.qpid.jms.transports.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSslOptions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality of the Netty based SSL Transport.
 */
public class NettySslTransportTest extends NettyTcpTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(NettySslTransportTest.class);

    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/example-jks.keystore";
    public static final String CLIENT_TRUSTSTORE = "src/test/resources/exanple-jks.truststore";
    public static final String KEYSTORE_TYPE = "jks";

    @Override
    @Test(timeout = 60 * 1000)
    public void testCreateWithNullOptionsUsesDefaults() throws Exception {
        URI serverLocation = new URI("tcp://localhost:5762");

        Transport transport = createTransport(serverLocation, testListener, null);
        assertEquals(TransportSslOptions.INSTANCE, transport.getTransportOptions());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWithoutTrustStoreFails() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptionsWithoutTrustStore(false, false));
            try {
                transport.connect();
                fail("Should not have connected to the server");
            } catch (Exception e) {
                LOG.info("Connection failed to untrusted test server.");
            }

            assertFalse(transport.isConnected());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerClientTrustsAll() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createClientOptionsWithoutTrustStore(true, false));
            try {
                transport.connect();
                LOG.info("Connection established to untrusted test server.");
            } catch (Exception e) {
                fail("Should have connected to the server");
            }

            assertTrue(transport.isConnected());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerVerifyHost() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportSslOptions clientOptions = createClientOptionsIsVerify(true);
            assertTrue("Expected verifyHost to be true", clientOptions.isVerifyHost());

            Transport transport = createTransport(serverLocation, testListener, clientOptions);
            try {
                transport.connect();
                fail("Should not have connected to the server");
            } catch (Exception e) {
                LOG.info("Connection failed to test server as expected.");
            }

            assertFalse(transport.isConnected());

            transport.close();
        }
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerNoVerifyHost() throws Exception {
        try (NettyEchoServer server = new NettyEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportSslOptions clientOptions = createClientOptionsIsVerify(false);
            assertFalse("Expected verifyHost to be false", clientOptions.isVerifyHost());

            Transport transport = createTransport(serverLocation, testListener, clientOptions);
            try {
                transport.connect();
                LOG.info("Connection established to test server.");
            } catch (Exception e) {
                fail("Should have connected to the server");
            }

            assertTrue(transport.isConnected());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Override
    protected Transport createTransport(URI serverLocation, TransportListener listener, TransportOptions options) {
        return new NettySslTransport(listener, serverLocation, options);
    }

    @Override
    protected TransportSslOptions createClientOptions() {
        return createClientOptionsIsVerify(false);
    }

    protected TransportSslOptions createClientOptionsIsVerify(boolean verifyHost) {
        TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

        options.setKeyStoreLocation(SERVER_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStoreLocation(CLIENT_TRUSTSTORE);
        options.setTrustStorePassword(PASSWORD);
        options.setStoreType(KEYSTORE_TYPE);
        options.setVerifyHost(verifyHost);

        return options;
    }

    @Override
    protected TransportSslOptions createServerOptions() {
        TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

        options.setKeyStoreLocation(SERVER_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStoreLocation(CLIENT_TRUSTSTORE);
        options.setTrustStorePassword(PASSWORD);
        options.setStoreType(KEYSTORE_TYPE);

        return options;
    }

    protected TransportSslOptions createClientOptionsWithoutTrustStore(boolean trustAll, boolean verifyHost) {
        TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

        options.setStoreType(KEYSTORE_TYPE);
        options.setTrustAll(trustAll);
        options.setVerifyHost(verifyHost);

        return options;
    }
}
