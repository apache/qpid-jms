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
package org.apache.qpid.jms;

import java.net.URI;

import org.apache.qpid.jms.provider.Provider;

/**
 * SSL Aware Factory class that allows for configuration of the SSL values used
 * in the Provider transports that are SSL aware.
 */
public class JmsSslConnectionFactory extends JmsConnectionFactory {

    private final JmsSslContext configured = JmsSslContext.getCurrentSslContext();

    public JmsSslConnectionFactory() {
    }

    public JmsSslConnectionFactory(String username, String password) {
        super(username, password);
    }

    public JmsSslConnectionFactory(String brokerURI) {
        super(brokerURI);
    }

    public JmsSslConnectionFactory(URI brokerURI) {
        super(brokerURI);
    }

    public JmsSslConnectionFactory(String username, String password, URI brokerURI) {
        super(username, password, brokerURI);
    }

    public JmsSslConnectionFactory(String username, String password, String brokerURI) {
        super(username, password, brokerURI);
    }

    @Override
    protected Provider createProvider(URI brokerURI) throws Exception {
        // Create and set a new instance as the current JmsSslContext for this thread
        // based on current configuration settings.
        JmsSslContext.setCurrentSslContext(configured.copy());
        return super.createProvider(brokerURI);
    }

    public String getKeyStoreLocation() {
        return configured.getKeyStoreLocation();
    }

    public void setKeyStoreLocation(String keyStoreLocation) {
        this.configured.setKeyStoreLocation(keyStoreLocation);
    }

    public String getKeyStorePassword() {
        return configured.getKeyStorePassword();
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.configured.setKeyStorePassword(keyStorePassword);
    }

    public String getTrustStoreLocation() {
        return configured.getTrustStoreLocation();
    }

    public void setTrustStoreLocation(String trustStoreLocation) {
        this.configured.setTrustStoreLocation(trustStoreLocation);
    }

    public String getTrustStorePassword() {
        return configured.getTrustStorePassword();
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.configured.setTrustStorePassword(trustStorePassword);
    }
}
