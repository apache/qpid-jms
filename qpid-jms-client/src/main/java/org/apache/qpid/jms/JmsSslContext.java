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

/**
 * Provides a wrapper around the SSL settings that are used by Provider transport
 * instances that use an SSL encryption layer.
 */
public class JmsSslContext {

    private String keyStoreLocation;
    private String keyStorePassword;
    private String trustStoreLocation;
    private String trustStorePassword;

    private static final JmsSslContext initial = new JmsSslContext();
    private static final ThreadLocal<JmsSslContext> current;

    static {

        initial.setKeyStoreLocation(System.getProperty("javax.net.ssl.keyStore"));
        initial.setKeyStorePassword(System.getProperty("javax.net.ssl.keyStorePassword"));
        initial.setTrustStoreLocation(System.getProperty("javax.net.ssl.trustStore"));
        initial.setTrustStorePassword(System.getProperty("javax.net.ssl.keyStorePassword"));

        current = new ThreadLocal<JmsSslContext>() {

            @Override
            protected JmsSslContext initialValue() {
                return initial;
            }
        };
    }

    protected JmsSslContext() {
    }

    public JmsSslContext copy() {
        JmsSslContext result = new JmsSslContext();
        result.setKeyStoreLocation(keyStoreLocation);
        result.setKeyStorePassword(keyStorePassword);
        result.setTrustStoreLocation(trustStoreLocation);
        result.setTrustStorePassword(trustStorePassword);
        return result;
    }

    static public void setCurrentSslContext(JmsSslContext bs) {
        current.set(bs);
    }

    static public JmsSslContext getCurrentSslContext() {
        return current.get();
    }

    public String getKeyStoreLocation() {
        return keyStoreLocation;
    }

    public void setKeyStoreLocation(String keyStoreLocation) {
        this.keyStoreLocation = keyStoreLocation;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getTrustStoreLocation() {
        return trustStoreLocation;
    }

    public void setTrustStoreLocation(String trustStoreLocation) {
        this.trustStoreLocation = trustStoreLocation;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }
}
