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
package org.apache.qpid.jms.transports;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslEngine;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.UnrecoverableKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for the TransportSupport class.
 */
public class TransportSupportTest extends QpidJmsTestCase {

    public static final String PASSWORD = "password";

    public static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    public static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";

    public static final String CLIENT_JCEKS_KEYSTORE = "src/test/resources/client-jceks.keystore";
    public static final String CLIENT_JCEKS_TRUSTSTORE = "src/test/resources/client-jceks.truststore";

    public static final String BROKER_PKCS12_KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
    public static final String BROKER_PKCS12_TRUSTSTORE = "src/test/resources/broker-pkcs12.truststore";
    public static final String CLIENT_PKCS12_KEYSTORE = "src/test/resources/client-pkcs12.keystore";
    public static final String CLIENT_PKCS12_TRUSTSTORE = "src/test/resources/client-pkcs12.truststore";

    public static final String KEYSTORE_JKS_TYPE = "jks";
    public static final String KEYSTORE_JCEKS_TYPE = "jceks";
    public static final String KEYSTORE_PKCS12_TYPE = "pkcs12";

    public static final String KEYSTORE_SYSTEM_PROPERTY = "keystore.base64";
    public static final String TRUSTSTORE_SYSTEM_PROPERTY = "truststore.base64";

    public static final String BAD_STORE_CONTENT = "bad-store-content";

    public static final String[] ENABLED_PROTOCOLS = new String[] { "TLSv1" };

    // Currently the OpenSSL implementation cannot disable SSLv2Hello
    public static final String[] ENABLED_OPENSSL_PROTOCOLS = new String[] { "SSLv2Hello", "TLSv1" };

    private static final String ALIAS_DOES_NOT_EXIST = "alias.does.not.exist";
    private static final String ALIAS_CA_CERT = "ca";

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testLegacySslProtocolsDisabledByDefaultJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(null, useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.contains("SSLv3"), "SSLv3 should not be enabled by default");
        assertFalse(engineProtocols.contains("SSLv2Hello"), "SSLv2Hello should not be enabled by default");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testLegacySslProtocolsDisabledByDefaultOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createJksSslOptions(null, useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.contains("SSLv3"), "SSLv3 should not be enabled by default");

        // TODO - Netty is currently unable to disable OpenSSL SSLv2Hello so we are stuck with it for now.
        // assertFalse("SSLv2Hello should not be enabled by default", engineProtocols.contains("SSLv2Hello"));
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextJksStoreJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextJksStoreOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createJksSslOptions(useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        // TODO There is no means currently of getting the protocol from the netty SslContext.
        // assertEquals("TLS", context.getProtocol());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextJksStoreWithConfiguredContextProtocolJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(useStoreLocation);
        String contextProtocol = "TLSv1.2";
        options.setContextProtocol(contextProtocol);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        assertEquals(contextProtocol, context.getProtocol());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextJksStoreWithConfiguredContextProtocolOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createJksSslOptions(useStoreLocation);
        String contextProtocol = "TLSv1.2";
        options.setContextProtocol(contextProtocol);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        // TODO There is no means currently of getting the protocol from the netty SslContext.
        // assertEquals(contextProtocol, context.getProtocol());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextNoKeyStorePasswordJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(useStoreLocation);
        options.setKeyStorePassword(null);
        try {
            TransportSupport.createJdkSslContext(options);
            fail("Expected an exception to be thrown");
        } catch (UnrecoverableKeyException e) {
            // Expected
        } catch (IllegalArgumentException iae) {
            // Expected in certain cases
            String message = iae.getMessage();
            assertTrue(message.contains("password can't be null"), "Unexpected message: " + message);
        }
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextNoKeyStorePasswordOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createJksSslOptions(useStoreLocation);
        options.setKeyStorePassword(null);

        try {
            TransportSupport.createOpenSslContext(options);
            fail("Expected an exception to be thrown");
        } catch (UnrecoverableKeyException e) {
            // Expected
        } catch (IllegalArgumentException iae) {
            // Expected in certain cases
            String message = iae.getMessage();
            assertTrue(message.contains("password can't be null"), "Unexpected message: " + message);
        }
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextWrongKeyStorePasswordJDK(boolean useStoreLocation) throws Exception {
        assertThrows(IOException.class, () -> {
            TransportOptions options = createJksSslOptions(useStoreLocation);
            options.setKeyStorePassword("wrong");
            TransportSupport.createJdkSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextWrongKeyStorePasswordOpenSSL(boolean useStoreLocation) throws Exception {
        assertThrows(IOException.class, () -> {
            assumeTrue(OpenSsl.isAvailable());
            assumeTrue(OpenSsl.supportsKeyManagerFactory());

            TransportOptions options = createJksSslOptions(useStoreLocation);
            options.setKeyStorePassword("wrong");
            TransportSupport.createOpenSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextBadPathToKeyStoreJDK(boolean useStoreLocation) throws Exception {
        Class<? extends Exception> expectedException = useStoreLocation ? IOException.class : IllegalArgumentException.class;
        assertThrows(expectedException, () -> {
            TransportOptions options = createJksSslOptions(useStoreLocation);
            if (useStoreLocation) {
                options.setKeyStoreLocation(CLIENT_JKS_KEYSTORE + ".bad");
            } else {
                System.setProperty(KEYSTORE_SYSTEM_PROPERTY, BAD_STORE_CONTENT);
            }
            TransportSupport.createJdkSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextBadPathToKeyStoreOpenSSL(boolean useStoreLocation) throws Exception {
        Class<? extends Exception> expectedException = useStoreLocation ? IOException.class : IllegalArgumentException.class;
        assertThrows(expectedException, () -> {
            assumeTrue(OpenSsl.isAvailable());
            assumeTrue(OpenSsl.supportsKeyManagerFactory());

            TransportOptions options = createJksSslOptions(useStoreLocation);
            if (useStoreLocation) {
                options.setKeyStoreLocation(CLIENT_JKS_KEYSTORE + ".bad");
            } else {
                System.setProperty(KEYSTORE_SYSTEM_PROPERTY, BAD_STORE_CONTENT);
            }
            TransportSupport.createOpenSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextWrongTrustStorePasswordJDK(boolean useStoreLocation) throws Exception {
        assertThrows(IOException.class, () -> {
            TransportOptions options = createJksSslOptions(useStoreLocation);
            options.setTrustStorePassword("wrong");
            TransportSupport.createJdkSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextWrongTrustStorePasswordOpenSSL(boolean useStoreLocation) throws Exception {
        assertThrows(IOException.class, () -> {
            assumeTrue(OpenSsl.isAvailable());
            assumeTrue(OpenSsl.supportsKeyManagerFactory());

            TransportOptions options = createJksSslOptions(useStoreLocation);
            options.setTrustStorePassword("wrong");
            TransportSupport.createOpenSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextBadPathToTrustStoreJDK(boolean useStoreLocation) throws Exception {
        Class<? extends Exception> expectedException = useStoreLocation ? IOException.class : IllegalArgumentException.class;
        assertThrows(expectedException, () -> {
            TransportOptions options = createJksSslOptions(useStoreLocation);
            if (useStoreLocation) {
                options.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE + ".bad");
            } else {
                System.setProperty(TRUSTSTORE_SYSTEM_PROPERTY, BAD_STORE_CONTENT);
            }
            TransportSupport.createJdkSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextBadPathToTrustStoreOpenSSL(boolean useStoreLocation) throws Exception {
        Class<? extends Exception> expectedException = useStoreLocation ? IOException.class : IllegalArgumentException.class;
        assertThrows(expectedException, () -> {
            assumeTrue(OpenSsl.isAvailable());
            assumeTrue(OpenSsl.supportsKeyManagerFactory());

            TransportOptions options = createJksSslOptions(useStoreLocation);
            if (useStoreLocation) {
                options.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE + ".bad");
            } else {
                System.setProperty(TRUSTSTORE_SYSTEM_PROPERTY, BAD_STORE_CONTENT);
            }
            TransportSupport.createOpenSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextJceksStoreJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJceksSslOptions(useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextJceksStoreOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createJceksSslOptions(useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);
        assertTrue(context.isClient());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextPkcs12StoreJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createPkcs12SslOptions(useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextPkcs12StoreOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createPkcs12SslOptions(useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);
        assertTrue(context.isClient());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextIncorrectStoreTypeJDK(boolean useStoreLocation) throws Exception {
        assertThrows(IOException.class, () -> {
            TransportOptions options = createPkcs12SslOptions(useStoreLocation);
            options.setStoreType(KEYSTORE_JCEKS_TYPE);
            TransportSupport.createJdkSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextIncorrectStoreTypeOpenSSL(boolean useStoreLocation) throws Exception {
        assertThrows(IOException.class, () -> {
            assumeTrue(OpenSsl.isAvailable());
            assumeTrue(OpenSsl.supportsKeyManagerFactory());

            TransportOptions options = createPkcs12SslOptions(useStoreLocation);
            options.setStoreType(KEYSTORE_JCEKS_TYPE);
            TransportSupport.createOpenSslContext(options);
        });
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromPkcs12StoreJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createPkcs12SslOptions(useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromPkcs12StoreOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createPkcs12SslOptions(useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromPkcs12StoreWithExplicitEnabledProtocolsJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createPkcs12SslOptions(ENABLED_PROTOCOLS, useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        assertArrayEquals(ENABLED_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromPkcs12StoreWithExplicitEnabledProtocolsOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createPkcs12SslOptions(ENABLED_PROTOCOLS, useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        assertArrayEquals(ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createJksSslOptions(useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledProtocolsJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(ENABLED_PROTOCOLS, useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        assertArrayEquals(ENABLED_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledProtocolsOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createJksSslOptions(ENABLED_PROTOCOLS, useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        assertArrayEquals(ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledProtocolsJDK(boolean useStoreLocation) throws Exception {
        // Discover the default enabled protocols
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue(protocols.length > 0, "There were no initial protocols to choose from!");

        // Pull out one to disable specifically
        String[] disabledProtocol = new String[] { protocols[protocols.length - 1] };
        String[] trimmedProtocols = Arrays.copyOf(protocols, protocols.length - 1);
        options.setDisabledProtocols(disabledProtocol);
        SSLContext context = TransportSupport.createJdkSslContext(options);
        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(trimmedProtocols, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledProtocolsOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled protocols
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue(protocols.length > 0, "There were no initial protocols to choose from!");

        // Pull out one to disable specifically
        String[] disabledProtocol = new String[] { protocols[protocols.length - 1] };
        String[] trimmedProtocols = Arrays.copyOf(protocols, protocols.length - 1);
        options.setDisabledProtocols(disabledProtocol);
        SslContext context = TransportSupport.createOpenSslContext(options);
        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(trimmedProtocols, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledProtocolsJDK(boolean useStoreLocation) throws Exception {
        // Discover the default enabled protocols
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assumeTrue(protocols.length > 1 , "Insufficient initial protocols to filter from: " + Arrays.toString(protocols));

        // Pull out two to enable, and one to disable specifically
        String protocol1 = protocols[0];
        String protocol2 = protocols[1];
        String[] enabledProtocols = new String[] { protocol1, protocol2 };
        String[] disabledProtocol = new String[] { protocol1 };
        String[] remainingProtocols = new String[] { protocol2 };
        options.setEnabledProtocols(enabledProtocols);
        options.setDisabledProtocols(disabledProtocol);
        SSLContext context = TransportSupport.createJdkSslContext(options);
        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);

        // verify the option took effect, that the disabled protocols were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals(remainingProtocols, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledProtocolsOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled protocols
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assumeTrue(protocols.length > 1 , "Insufficient initial protocols to filter from: " + Arrays.toString(protocols));

        // Pull out two to enable, and one to disable specifically
        String protocol1 = protocols[0];
        String protocol2 = protocols[1];
        String[] enabledProtocols = new String[] { protocol1, protocol2 };
        String[] disabledProtocol = new String[] { protocol1 };
        String[] remainingProtocols = new String[] { protocol2 };
        options.setEnabledProtocols(enabledProtocols);
        options.setDisabledProtocols(disabledProtocol);
        SslContext context = TransportSupport.createOpenSslContext(options);
        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);

        // Because Netty cannot currently disable SSLv2Hello in OpenSSL we need to account for it popping up.
        ArrayList<String> remainingProtocolsList = new ArrayList<>(Arrays.asList(remainingProtocols));
        if (!remainingProtocolsList.contains("SSLv2Hello")) {
            remainingProtocolsList.add(0, "SSLv2Hello");
        }

        remainingProtocols = remainingProtocolsList.toArray(new String[remainingProtocolsList.size()]);

        // verify the option took effect, that the disabled protocols were removed from the enabled list.
        assertNotNull(engine);
        assertEquals(remainingProtocolsList.size(), engine.getEnabledProtocols().length, "Enabled protocols not as expected");
        assertTrue(remainingProtocolsList.containsAll(Arrays.asList(engine.getEnabledProtocols())), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledCiphersJDK(boolean useStoreLocation) throws Exception {
        // Discover the default enabled ciphers
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 0, "There were no initial ciphers to choose from!");

        // Pull out one to enable specifically
        String cipher = ciphers[0];
        String[] enabledCipher = new String[] { cipher };
        options.setEnabledCipherSuites(enabledCipher);
        SSLContext context = TransportSupport.createJdkSslContext(options);
        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(enabledCipher, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledCiphersOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 0, "There were no initial ciphers to choose from!");

        // Pull out one to enable specifically
        String cipher = ciphers[0];
        String[] enabledCipher = new String[] { cipher };
        options.setEnabledCipherSuites(enabledCipher);
        SslContext context = TransportSupport.createOpenSslContext(options);
        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(enabledCipher, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledCiphersJDK(boolean useStoreLocation) throws Exception {
        // Discover the default enabled ciphers
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 0, "There were no initial ciphers to choose from!");

        // Pull out one to disable specifically
        String[] disabledCipher = new String[] { ciphers[ciphers.length - 1] };
        String[] trimmedCiphers = Arrays.copyOf(ciphers, ciphers.length - 1);
        options.setDisabledCipherSuites(disabledCipher);
        SSLContext context = TransportSupport.createJdkSslContext(options);
        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(trimmedCiphers, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledCiphersOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 0, "There were no initial ciphers to choose from!");

        // Pull out one to disable specifically
        String[] disabledCipher = new String[] { ciphers[ciphers.length - 1] };
        String[] trimmedCiphers = Arrays.copyOf(ciphers, ciphers.length - 1);
        options.setDisabledCipherSuites(disabledCipher);
        SslContext context = TransportSupport.createOpenSslContext(options);
        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(trimmedCiphers, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledCiphersJDK(boolean useStoreLocation) throws Exception {
        // Discover the default enabled ciphers
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 1, "There werent enough initial ciphers to choose from!");

        // Pull out two to enable, and one to disable specifically
        String cipher1 = ciphers[0];
        String cipher2 = ciphers[1];
        String[] enabledCiphers = new String[] { cipher1, cipher2 };
        String[] disabledCipher = new String[] { cipher1 };
        String[] remainingCipher = new String[] { cipher2 };
        options.setEnabledCipherSuites(enabledCiphers);
        options.setDisabledCipherSuites(disabledCipher);
        SSLContext context = TransportSupport.createJdkSslContext(options);
        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);

        // verify the option took effect, that the disabled ciphers were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals(remainingCipher, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledCiphersOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        TransportOptions options = createJksSslOptions(useStoreLocation);
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 1, "There werent enough initial ciphers to choose from!");

        // Pull out two to enable, and one to disable specifically
        String cipher1 = ciphers[0];
        String cipher2 = ciphers[1];
        String[] enabledCiphers = new String[] { cipher1, cipher2 };
        String[] disabledCipher = new String[] { cipher1 };
        String[] remainingCipher = new String[] { cipher2 };
        options.setEnabledCipherSuites(enabledCiphers);
        options.setDisabledCipherSuites(disabledCipher);
        SslContext context = TransportSupport.createOpenSslContext(options);
        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);

        // verify the option took effect, that the disabled ciphers were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals(remainingCipher, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJceksStoreJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJceksSslOptions(useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJceksStoreOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = createJceksSslOptions(useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJceksStoreWithExplicitEnabledProtocolsJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJceksSslOptions(ENABLED_PROTOCOLS, useStoreLocation);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        assertArrayEquals(ENABLED_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineFromJceksStoreWithExplicitEnabledProtocolsOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Try and disable all but the one we really want but for now expect that this one plus SSLv2Hello
        // is going to come back until the netty code can successfully disable them all.
        TransportOptions options = createJceksSslOptions(ENABLED_PROTOCOLS, useStoreLocation);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        assertArrayEquals(ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineWithVerifyHostJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(useStoreLocation);
        options.setVerifyHost(true);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineWithVerifyHostOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        TransportOptions options = createJksSslOptions(useStoreLocation);
        options.setVerifyHost(true);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineWithoutVerifyHostJDK(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(useStoreLocation);
        options.setVerifyHost(false);

        SSLContext context = TransportSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
        assertNotNull(engine);

        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslEngineWithoutVerifyHostOpenSSL(boolean useStoreLocation) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        TransportOptions options = createJksSslOptions(useStoreLocation);
        options.setVerifyHost(false);

        SslContext context = TransportSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, context, options);
        assertNotNull(engine);

        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextWithKeyAliasWhichDoesntExist(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(useStoreLocation);
        options.setKeyAlias(ALIAS_DOES_NOT_EXIST);

        try {
            TransportSupport.createJdkSslContext(options);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @ValueSource(booleans = { true, false })
    @ParameterizedTest(name = "{index}: useStoreLocation={0}")
    public void testCreateSslContextWithKeyAliasWhichRepresentsNonKeyEntry(boolean useStoreLocation) throws Exception {
        TransportOptions options = createJksSslOptions(useStoreLocation);
        options.setKeyAlias(ALIAS_CA_CERT);

        try {
            TransportSupport.createJdkSslContext(options);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    @Timeout(100)
    public void testIsOpenSSLPossible() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = new TransportOptions();
        options.setUseOpenSSL(false);
        assertFalse(TransportSupport.isOpenSSLPossible(options));

        options.setUseOpenSSL(true);
        assertTrue(TransportSupport.isOpenSSLPossible(options));
    }

    @Test
    @Timeout(100)
    public void testIsOpenSSLPossibleWhenHostNameVerificationConfigured() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        TransportOptions options = new TransportOptions();
        options.setUseOpenSSL(true);

        options.setVerifyHost(false);
        assertTrue(TransportSupport.isOpenSSLPossible(options));

        options.setVerifyHost(true);
        assertTrue(TransportSupport.isOpenSSLPossible(options));
    }

    @Test
    @Timeout(100)
    public void testIsOpenSSLPossibleWhenKeyAliasIsSpecified() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        TransportOptions options = new TransportOptions();
        options.setUseOpenSSL(true);
        options.setKeyAlias("alias");

        assertFalse(TransportSupport.isOpenSSLPossible(options));
    }

    @Test
    @Timeout(100)
    public void testCreateSslHandlerJDK() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = new TransportOptions();
        options.setUseOpenSSL(false);

        SslHandler handler = TransportSupport.createSslHandler(null, null, options);
        assertNotNull(handler);
        assertFalse(handler.engine() instanceof OpenSslEngine);
    }

    @Test
    @Timeout(100)
    public void testCreateSslHandlerOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = new TransportOptions();
        options.setUseOpenSSL(true);

        SslHandler handler = TransportSupport.createSslHandler(PooledByteBufAllocator.DEFAULT, null, options);
        assertNotNull(handler);
        assertTrue(handler.engine() instanceof OpenSslEngine);
    }

    @Test
    @Timeout(100)
    public void testCreateOpenSSLEngineFailsWhenAllocatorMissing() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        TransportOptions options = new TransportOptions();
        options.setUseOpenSSL(true);

        SslContext context = TransportSupport.createOpenSslContext(options);
        try {
            TransportSupport.createOpenSslEngine(null, null, context, options);
            fail("Should throw IllegalArgumentException for null allocator.");
        } catch (IllegalArgumentException iae) {}
    }

    private TransportOptions createJksSslOptions(boolean useStoreLocation) throws IOException {
        return createJksSslOptions(null, useStoreLocation);
    }

    private TransportOptions createJksSslOptions(String[] enabledProtocols, boolean useStoreLocation) throws IOException {
        TransportOptions options = new TransportOptions();

        if (useStoreLocation) {
            options.setKeyStoreLocation(CLIENT_JKS_KEYSTORE);
            options.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE);
        } else {
            System.setProperty(KEYSTORE_SYSTEM_PROPERTY, readStoreBase64Content(CLIENT_JKS_KEYSTORE));
            System.setProperty(TRUSTSTORE_SYSTEM_PROPERTY, readStoreBase64Content(CLIENT_JKS_TRUSTSTORE));
            options.setKeyStoreBase64Property(KEYSTORE_SYSTEM_PROPERTY);
            options.setTrustStoreBase64Property(TRUSTSTORE_SYSTEM_PROPERTY);
        }
        options.setStoreType(KEYSTORE_JKS_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private TransportOptions createJceksSslOptions(boolean useStoreLocation) throws IOException {
        return createJceksSslOptions(null, useStoreLocation);
    }

    private TransportOptions createJceksSslOptions(String[] enabledProtocols, boolean useStoreLocation) throws IOException {
        TransportOptions options = new TransportOptions();

        if (useStoreLocation) {
            options.setKeyStoreLocation(CLIENT_JCEKS_KEYSTORE);
            options.setTrustStoreLocation(CLIENT_JCEKS_TRUSTSTORE);
        } else {
            System.setProperty(KEYSTORE_SYSTEM_PROPERTY, readStoreBase64Content(CLIENT_JCEKS_KEYSTORE));
            System.setProperty(TRUSTSTORE_SYSTEM_PROPERTY, readStoreBase64Content(CLIENT_JCEKS_TRUSTSTORE));
            options.setKeyStoreBase64Property(KEYSTORE_SYSTEM_PROPERTY);
            options.setTrustStoreBase64Property(TRUSTSTORE_SYSTEM_PROPERTY);
        }
        options.setStoreType(KEYSTORE_JCEKS_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private TransportOptions createPkcs12SslOptions(boolean useStoreLocation) throws IOException {
        return createPkcs12SslOptions(null, useStoreLocation);
    }

    private TransportOptions createPkcs12SslOptions(String[] enabledProtocols, boolean useStoreLocation) throws IOException {
        TransportOptions options = new TransportOptions();

        if (useStoreLocation) {
            options.setKeyStoreLocation(CLIENT_PKCS12_KEYSTORE);
            options.setTrustStoreLocation(CLIENT_PKCS12_TRUSTSTORE);
        } else {
            System.setProperty(KEYSTORE_SYSTEM_PROPERTY, readStoreBase64Content(CLIENT_PKCS12_KEYSTORE));
            System.setProperty(TRUSTSTORE_SYSTEM_PROPERTY, readStoreBase64Content(CLIENT_PKCS12_TRUSTSTORE));
            options.setKeyStoreBase64Property(KEYSTORE_SYSTEM_PROPERTY);
            options.setTrustStoreBase64Property(TRUSTSTORE_SYSTEM_PROPERTY);
        }
        options.setStoreType(KEYSTORE_PKCS12_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private SSLEngine createSSLEngineDirectly(TransportOptions options) throws Exception {
        SSLContext context = TransportSupport.createJdkSslContext(options);
        SSLEngine engine = context.createSSLEngine();
        return engine;
    }

    private SSLEngine createOpenSSLEngineDirectly(TransportOptions options) throws Exception {
        SslContext context = TransportSupport.createOpenSslContext(options);
        SSLEngine engine = context.newEngine(PooledByteBufAllocator.DEFAULT);
        return engine;
    }

    private String readStoreBase64Content(String storePath) throws IOException {
        byte[] storeBytes = Files.readAllBytes(Paths.get(storePath));
        return Base64.getEncoder().encodeToString(storeBytes);
    }
}
