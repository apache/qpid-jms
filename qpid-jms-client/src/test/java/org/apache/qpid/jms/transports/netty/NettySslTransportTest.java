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

import java.net.URI;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSslOptions;

/**
 * Test basic functionality of the Netty based SSL Transport.
 */
public class NettySslTransportTest extends NettyTcpTransportTest {

    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/broker-jks.keystore";
    public static final String SERVER_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    public static final String CLINET_KEYSTORE = "src/test/resources/client-jks.keystore";
    public static final String CLINET_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    public static final String KEYSTORE_TYPE = "jks";

    @Override
    protected Transport createTransport(URI serverLocation, TransportListener listener, TransportOptions options) {
        return new NettySslTransport(listener, serverLocation, options);
    }

    @Override
    protected TransportSslOptions createClientOptions() {
        TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

        options.setKeyStoreLocation(CLINET_KEYSTORE);
        options.setTrustStoreLocation(CLINET_TRUSTSTORE);
        options.setStoreType(KEYSTORE_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);

        return options;
    }

    @Override
    protected TransportSslOptions createServerOptions() {
        TransportSslOptions options = TransportSslOptions.INSTANCE.clone();

        options.setKeyStoreLocation(SERVER_KEYSTORE);
        options.setTrustStoreLocation(SERVER_TRUSTSTORE);
        options.setStoreType(KEYSTORE_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);

        return options;
    }
}
