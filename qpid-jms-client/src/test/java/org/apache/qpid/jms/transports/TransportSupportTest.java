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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.UnrecoverableKeyException;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

/**
 * Tests for the TransportSupport class.
 */
public class TransportSupportTest extends QpidJmsTestCase {

    public static final String PASSWORD = "password";

    public static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    public static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    public static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    public static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";

    public static final String BROKER_JCEKS_KEYSTORE = "src/test/resources/broker-jceks.keystore";
    public static final String BROKER_JCEKS_TRUSTSTORE = "src/test/resources/broker-jceks.truststore";
    public static final String CLIENT_JCEKS_KEYSTORE = "src/test/resources/client-jceks.keystore";
    public static final String CLIENT_JCEKS_TRUSTSTORE = "src/test/resources/client-jceks.truststore";

    public static final String BROKER_PKCS12_KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
    public static final String BROKER_PKCS12_TRUSTSTORE = "src/test/resources/broker-pkcs12.truststore";
    public static final String CLIENT_PKCS12_KEYSTORE = "src/test/resources/client-pkcs12.keystore";
    public static final String CLIENT_PKCS12_TRUSTSTORE = "src/test/resources/client-pkcs12.truststore";

    public static final String KEYSTORE_JKS_TYPE = "jks";
    public static final String KEYSTORE_JCEKS_TYPE = "jceks";
    public static final String KEYSTORE_PKCS12_TYPE = "pkcs12";

    public static final String[] ENABLED_PROTOCOLS = new String[] { "TLSv1" };

    private static final String ALIAS_DOES_NOT_EXIST = "alias.does.not.exist";
    private static final String ALIAS_CA_CERT = "ca";

    @Test
    public void testLegacySslProtocolsDisabledByDefault() throws Exception {
        TransportSslOptions options = createJksSslOptions(null);

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse("SSLv3 should not be enabled by default", engineProtocols.contains("SSLv3"));
        assertFalse("SSLv2Hello should not be enabled by default", engineProtocols.contains("SSLv2Hello"));
    }

    @Test
    public void testCreateSslContextJksStore() throws Exception {
        TransportSslOptions options = createJksSslOptions();

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @Test
    public void testCreateSslContextJksStoreWithConfiguredContextProtocol() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        String contextProtocol = "TLSv1.2";
        options.setContextProtocol(contextProtocol);

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        assertEquals(contextProtocol, context.getProtocol());
    }

    @Test(expected = UnrecoverableKeyException.class)
    public void testCreateSslContextNoKeyStorePassword() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setKeyStorePassword(null);
        TransportSupport.createSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextWrongKeyStorePassword() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setKeyStorePassword("wrong");
        TransportSupport.createSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextBadPathToKeyStore() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setKeyStoreLocation(CLIENT_JKS_KEYSTORE + ".bad");
        TransportSupport.createSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextWrongTrustStorePassword() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setTrustStorePassword("wrong");
        TransportSupport.createSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextBadPathToTrustStore() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE + ".bad");
        TransportSupport.createSslContext(options);
    }

    @Test
    public void testCreateSslContextJceksStore() throws Exception {
        TransportSslOptions options = createJceksSslOptions();

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @Test
    public void testCreateSslContextPkcs12Store() throws Exception {
        TransportSslOptions options = createPkcs12SslOptions();

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextIncorrectStoreType() throws Exception {
        TransportSslOptions options = createPkcs12SslOptions();
        options.setStoreType(KEYSTORE_JCEKS_TYPE);
        TransportSupport.createSslContext(options);
    }

    @Test
    public void testCreateSslEngineFromPkcs12Store() throws Exception {
        TransportSslOptions options = createPkcs12SslOptions();

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromPkcs12StoreWithExplicitEnabledProtocols() throws Exception {
        TransportSslOptions options = createPkcs12SslOptions(ENABLED_PROTOCOLS);

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStore() throws Exception {
        TransportSslOptions options = createJksSslOptions();

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledProtocols() throws Exception {
        TransportSslOptions options = createJksSslOptions(ENABLED_PROTOCOLS);

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledProtocols() throws Exception {
        // Discover the default enabled protocols
        TransportSslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue("There were no initial protocols to choose from!", protocols.length > 0);

        // Pull out one to disable specifically
        String[] disabledProtocol = new String[] { protocols[protocols.length - 1] };
        String[] trimmedProtocols = Arrays.copyOf(protocols, protocols.length - 1);
        options.setDisabledProtocols(disabledProtocol);
        SSLContext context = TransportSupport.createSslContext(options);
        SSLEngine engine = TransportSupport.createSslEngine(context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled protocols not as expected", trimmedProtocols, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledProtocols() throws Exception {
        // Discover the default enabled protocols
        TransportSslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue("There were no initial protocols to choose from!", protocols.length > 1);

        // Pull out two to enable, and one to disable specifically
        String protocol1 = protocols[0];
        String protocol2 = protocols[1];
        String[] enabledProtocols = new String[] { protocol1, protocol2 };
        String[] disabledProtocol = new String[] { protocol1 };
        String[] remainingProtocols = new String[] { protocol2 };
        options.setEnabledProtocols(enabledProtocols);
        options.setDisabledProtocols(disabledProtocol);
        SSLContext context = TransportSupport.createSslContext(options);
        SSLEngine engine = TransportSupport.createSslEngine(context, options);

        // verify the option took effect, that the disabled protocols were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals("Enabled protocols not as expected", remainingProtocols, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledCiphers() throws Exception {
        // Discover the default enabled ciphers
        TransportSslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There were no initial ciphers to choose from!", ciphers.length > 0);

        // Pull out one to enable specifically
        String cipher = ciphers[0];
        String[] enabledCipher = new String[] { cipher };
        options.setEnabledCipherSuites(enabledCipher);
        SSLContext context = TransportSupport.createSslContext(options);
        SSLEngine engine = TransportSupport.createSslEngine(context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", enabledCipher, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledCiphers() throws Exception {
        // Discover the default enabled ciphers
        TransportSslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There were no initial ciphers to choose from!", ciphers.length > 0);

        // Pull out one to disable specifically
        String[] disabledCipher = new String[] { ciphers[ciphers.length - 1] };
        String[] trimmedCiphers = Arrays.copyOf(ciphers, ciphers.length - 1);
        options.setDisabledCipherSuites(disabledCipher);
        SSLContext context = TransportSupport.createSslContext(options);
        SSLEngine engine = TransportSupport.createSslEngine(context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", trimmedCiphers, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledCiphers() throws Exception {
        // Discover the default enabled ciphers
        TransportSslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There werent enough initial ciphers to choose from!", ciphers.length > 1);

        // Pull out two to enable, and one to disable specifically
        String cipher1 = ciphers[0];
        String cipher2 = ciphers[1];
        String[] enabledCiphers = new String[] { cipher1, cipher2 };
        String[] disabledCipher = new String[] { cipher1 };
        String[] remainingCipher = new String[] { cipher2 };
        options.setEnabledCipherSuites(enabledCiphers);
        options.setDisabledCipherSuites(disabledCipher);
        SSLContext context = TransportSupport.createSslContext(options);
        SSLEngine engine = TransportSupport.createSslEngine(context, options);

        // verify the option took effect, that the disabled ciphers were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", remainingCipher, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJceksStore() throws Exception {
        TransportSslOptions options = createJceksSslOptions();

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromJceksStoreWithExplicitEnabledProtocols() throws Exception {
        TransportSslOptions options = createJceksSslOptions(ENABLED_PROTOCOLS);

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineWithVerifyHost() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setVerifyHost(true);

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testCreateSslEngineWithoutVerifyHost() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setVerifyHost(false);

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testCreateSslContextWithKeyAliasWhichDoesntExist() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setKeyAlias(ALIAS_DOES_NOT_EXIST);

        try {
            TransportSupport.createSslContext(options);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testCreateSslContextWithKeyAliasWhichRepresentsNonKeyEntry() throws Exception {
        TransportSslOptions options = createJksSslOptions();
        options.setKeyAlias(ALIAS_CA_CERT);

        try {
            TransportSupport.createSslContext(options);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    private TransportSslOptions createJksSslOptions() {
        return createJksSslOptions(null);
    }

    private TransportSslOptions createJksSslOptions(String[] enabledProtocols) {
        TransportSslOptions options = new TransportSslOptions();

        options.setKeyStoreLocation(CLIENT_JKS_KEYSTORE);
        options.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE);
        options.setStoreType(KEYSTORE_JKS_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private TransportSslOptions createJceksSslOptions() {
        return createJceksSslOptions(null);
    }

    private TransportSslOptions createJceksSslOptions(String[] enabledProtocols) {
        TransportSslOptions options = new TransportSslOptions();

        options.setKeyStoreLocation(CLIENT_JCEKS_KEYSTORE);
        options.setTrustStoreLocation(CLIENT_JCEKS_TRUSTSTORE);
        options.setStoreType(KEYSTORE_JCEKS_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private TransportSslOptions createPkcs12SslOptions() {
        return createPkcs12SslOptions(null);
    }

    private TransportSslOptions createPkcs12SslOptions(String[] enabledProtocols) {
        TransportSslOptions options = new TransportSslOptions();

        options.setKeyStoreLocation(CLIENT_PKCS12_KEYSTORE);
        options.setTrustStoreLocation(CLIENT_PKCS12_TRUSTSTORE);
        options.setStoreType(KEYSTORE_PKCS12_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private SSLEngine createSSLEngineDirectly(TransportSslOptions options) throws Exception {
        SSLContext context = TransportSupport.createSslContext(options);
        SSLEngine engine = context.createSSLEngine();
        return engine;
    }

}
