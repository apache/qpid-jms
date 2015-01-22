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

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

/**
 * Tests for the TransmportSupport class.
 */
public class TransportSupportTest extends QpidJmsTestCase {

    public static final String PASSWORD = "password";
    public static final String BROKER_KEYSTORE = "src/test/resources/broker-jks.keystore";
    public static final String BROKER_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    public static final String CLINET_KEYSTORE = "src/test/resources/client-jks.keystore";
    public static final String CLINET_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    public static final String KEYSTORE_TYPE = "jks";

    @Test
    public void testCreateSslContext() throws Exception {
        TransportSslOptions options = createSslOptions();

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @Test
    public void testCreateSslEngine() throws Exception {
        TransportSslOptions options = createSslOptions();

        SSLContext context = TransportSupport.createSslContext(options);
        assertNotNull(context);

        SSLEngine engine = TransportSupport.createSslEngine(context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        List<String> defaultProtocols = Arrays.asList(TransportSslOptions.DEFAULT_ENABLED_PROTOCOLS);

        assertThat(engineProtocols, containsInAnyOrder(defaultProtocols.toArray()));
    }

    private TransportSslOptions createSslOptions() {
        TransportSslOptions options = new TransportSslOptions();

        options.setKeyStoreLocation(CLINET_KEYSTORE);
        options.setTrustStoreLocation(CLINET_TRUSTSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);

        return options;
    }
}
