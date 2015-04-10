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
package org.apache.qpid.jms.transports;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
        options.setStoreType(KEYSTORE_JKS_TYPE);
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
}
