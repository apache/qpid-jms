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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.lang.reflect.Field;
import java.net.URI;

import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslEngine;
import io.netty.handler.ssl.SslHandler;

/**
 * Test basic functionality of the Netty based TCP Transport ruuing in secure mode (SSL).
 */
@RunWith(QpidJMSTestRunner.class)
public class NettyOpenSslTransportTest extends NettySslTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(NettyOpenSslTransportTest.class);

    @Test(timeout = 240 * 1000)
    public void testConnectToServerWithOpenSSLEnabled() throws Exception {
        doTestOpenSSLSupport(true);
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWithOpenSSLDisabled() throws Exception {
        doTestOpenSSLSupport(false);
    }

    private void doTestOpenSSLSupport(boolean useOpenSSL) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportOptions options = createClientOptions();
            options.setUseOpenSSL(useOpenSSL);

            Transport transport = createTransport(serverLocation, testListener, options);
            try {
                transport.connect(null, null);
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());
            assertOpenSSL("Transport should be using OpenSSL", useOpenSSL, transport);

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testConnectToServerWithUserSuppliedSSLContextWorksWhenOpenSSLRequested() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        try (NettyEchoServer server = createEchoServer(createServerOptions())) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            TransportOptions options = new TransportOptions();

            options.setKeyStoreLocation(CLIENT_KEYSTORE);
            options.setKeyStorePassword(PASSWORD);
            options.setTrustStoreLocation(CLIENT_TRUSTSTORE);
            options.setTrustStorePassword(PASSWORD);
            options.setStoreType(KEYSTORE_TYPE);

            SSLContext sslContext = TransportSupport.createJdkSslContext(options);

            options = new TransportOptions();
            options.setVerifyHost(false);
            options.setUseOpenSSL(true);

            Transport transport = createTransport(serverLocation, testListener, options);
            try {
                transport.connect(null, sslContext);
                LOG.info("Connected to server:{} as expected.", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(serverLocation, transport.getRemoteLocation());
            assertOpenSSL("Transport should not be using OpenSSL", false, transport);

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportClosed);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    private void assertOpenSSL(String message, boolean expected, Transport transport) throws Exception {
        Field channel = null;
        Class<?> transportType = transport.getClass();

        while (transportType != null && channel == null) {
            try {
                channel = transportType.getDeclaredField("channel");
            } catch (NoSuchFieldException error) {
                transportType = transportType.getSuperclass();
                if (Object.class.equals(transportType)) {
                    transportType = null;
                }
            }
        }

        assertNotNull("Transport implementation unknown", channel);

        channel.setAccessible(true);

        Channel activeChannel = (Channel) channel.get(transport) ;
        ChannelHandler handler = activeChannel.pipeline().get("ssl");
        assertNotNull("Channel should have an SSL Handler registered");
        assertTrue(handler instanceof SslHandler);
        SslHandler sslHandler = (SslHandler) handler;

        if (expected) {
            assertTrue(message, sslHandler.engine() instanceof OpenSslEngine);
        } else {
            assertFalse(message, sslHandler.engine() instanceof OpenSslEngine);
        }
    }

    @Override
    @Ignore("Can't apply keyAlias in Netty OpenSSL impl")
    @Test(timeout = 60 * 1000)
    public void testConnectWithSpecificClientAuthKeyAlias() throws Exception {
        // TODO - Revert to superclass version if keyAlias becomes supported for Netty.
    }

    @Override
    protected TransportOptions createClientOptionsIsVerify(boolean verifyHost) {
        TransportOptions options = new TransportOptions();

        options.setUseOpenSSL(true);
        options.setKeyStoreLocation(CLIENT_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStoreLocation(CLIENT_TRUSTSTORE);
        options.setTrustStorePassword(PASSWORD);
        options.setStoreType(KEYSTORE_TYPE);
        options.setVerifyHost(verifyHost);

        return options;
    }

    @Override
    protected TransportOptions createClientOptionsWithoutTrustStore(boolean trustAll) {
        TransportOptions options = new TransportOptions();

        options.setStoreType(KEYSTORE_TYPE);
        options.setUseOpenSSL(true);
        options.setTrustAll(trustAll);

        return options;
    }
}
