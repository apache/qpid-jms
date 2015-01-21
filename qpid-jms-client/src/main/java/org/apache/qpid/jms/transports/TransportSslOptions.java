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

/**
 * Holds the defined SSL options for connections that operate over a secure
 * transport.  Options are read from the environment and can be overridden by
 * specifying them on the connection URI.
 */
public class TransportSslOptions extends TransportOptions {

    private static final String[] DEFAULT_ENABLED_PROTOCOLS = {"SSLv2Hello", "TLSv1", "TLSv1.1", "TLSv1.2"};
    private static final String DEFAULT_STORE_TYPE = "JKS";

    public static final TransportSslOptions INSTANCE = new TransportSslOptions();

    private String keyStoreLocation;
    private String keyStorePassword;
    private String trustStoreLocation;
    private String trustStorePassword;
    private String storeType = DEFAULT_STORE_TYPE;
    private String[] enabledCipherSuites;
    private String[] enabledProtocols = DEFAULT_ENABLED_PROTOCOLS;

    static {
        INSTANCE.setKeyStoreLocation(System.getProperty("javax.net.ssl.keyStore"));
        INSTANCE.setKeyStorePassword(System.getProperty("javax.net.ssl.keyStorePassword"));
        INSTANCE.setTrustStoreLocation(System.getProperty("javax.net.ssl.trustStore"));
        INSTANCE.setTrustStorePassword(System.getProperty("javax.net.ssl.keyStorePassword"));
    }

    /**
     * @return the keyStoreLocation currently configured.
     */
    public String getKeyStoreLocation() {
        return keyStoreLocation;
    }

    /**
     * Sets the location on disk of the key store to use.
     *
     * @param keyStoreLocation
     *        the keyStoreLocation to use to create the key manager.
     */
    public void setKeyStoreLocation(String keyStoreLocation) {
        this.keyStoreLocation = keyStoreLocation;
    }

    /**
     * @return the keyStorePassword
     */
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    /**
     * @param keyStorePassword the keyStorePassword to set
     */
    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    /**
     * @return the trustStoreLocation
     */
    public String getTrustStoreLocation() {
        return trustStoreLocation;
    }

    /**
     * @param trustStoreLocation the trustStoreLocation to set
     */
    public void setTrustStoreLocation(String trustStoreLocation) {
        this.trustStoreLocation = trustStoreLocation;
    }

    /**
     * @return the trustStorePassword
     */
    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    /**
     * @param trustStorePassword the trustStorePassword to set
     */
    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    /**
     * @return the storeType
     */
    public String getStoreType() {
        return storeType;
    }

    /**
     * @param storeType
     *        the format that the store files are encoded in.
     */
    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    /**
     * @return the enabledCipherSuites
     */
    public String[] getEnabledCipherSuites() {
        return enabledCipherSuites;
    }

    /**
     * @param enabledCipherSuites the enabledCipherSuites to set
     */
    public void setEnabledCipherSuites(String[] enabledCipherSuites) {
        this.enabledCipherSuites = enabledCipherSuites;
    }

    /**
     * @return the enabledProtocols
     */
    public String[] getEnabledProtocols() {
        return enabledProtocols;
    }

    /**
     * @param enabledProtocols the enabledProtocols to set
     */
    public void setEnabledProtocols(String[] enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
    }

    @Override
    public TransportSslOptions clone() {
        return copyOptions(new TransportSslOptions());
    }

    protected TransportSslOptions copyOptions(TransportSslOptions copy) {
        super.copyOptions(copy);

        copy.setKeyStoreLocation(getKeyStoreLocation());
        copy.setKeyStorePassword(getKeyStorePassword());
        copy.setTrustStoreLocation(getTrustStoreLocation());
        copy.setTrustStorePassword(getTrustStorePassword());
        copy.setStoreType(getStoreType());
        copy.setEnabledCipherSuites(getEnabledCipherSuites());
        copy.setEnabledProtocols(getEnabledProtocols());

        return copy;
    }
}
