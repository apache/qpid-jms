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
package org.apache.qpid.jms;

import java.net.URI;
import java.util.Map;
import java.util.function.BiFunction;

import javax.jms.Connection;
import javax.net.ssl.SSLContext;

/**
 * Connection Extensions Definitions
 * <p>
 * Connection Extensions all the user to apply functions that can override or update
 * client configuration based on state in their own applications such as providing a custom
 * SSLContext or updating an authentication token from an external provider on each attempt
 * to connect to a remote.
 * <p>
 * The extensions take the form of a BiFunction&lt;Connection, URI, Object&gt; passed into the
 * ConnectionFactory using the {@link JmsConnectionFactory#setExtension(String, BiFunction)}.
 */
public enum JmsConnectionExtensions {

    /**
     * Allows a user to inject a custom SSL Context into the client which overrides
     * the instance that the client would create and use.
     * <p>
     * Using this method overrides the effect of URI/System property configuration relating
     * to the location/credentials/type of SSL key/trust stores and whether to trust all
     * certificates or use a particular keyAlias.
     * <p>
     * The extension function takes the form of a BiFunction defined as the following:
     * <ul>
     *   <li>
     *     {@link BiFunction}&lt;{@link Connection}, {@link URI}, {@link SSLContext}&gt;
     *   </li>
     * </ul>
     */
    SSL_CONTEXT("sslContext"),

    /**
     * Allows a user to inject a custom user name into the client which overrides
     * the instance that the client would use to authenticate with the remote.
     * <p>
     * Using this method overrides the effect of URI/ConnectionFactory configuration relating
     * to the user name provided to the remote for authentication.  This method will be invoked
     * on each connection authentication attempt in the presence of a failover configuration.
     * <p>
     * The extension function takes the form of a BiFunction defined as the following:
     * <ul>
     *   <li>
     *     {@link BiFunction}&lt;{@link Connection}, {@link URI}, {@link String}&gt;
     *   </li>
     * </ul>
     */
    USERNAME_OVERRIDE("username"),

    /**
     * Allows a user to inject a custom password into the client which overrides
     * the instance that the client would use to authenticate with the remote.
     * <p>
     * Using this method overrides the effect of URI/ConnectionFactory configuration relating
     * to the password provided to the remote for authentication.  This method will be invoked
     * on each connection authentication attempt in the presence of a failover configuration.
     * <p>
     * The extension function takes the form of a BiFunction defined as the following:
     * <ul>
     *   <li>
     *     {@link BiFunction}&lt;{@link Connection}, {@link URI}, {@link String}&gt;
     *   </li>
     * </ul>
     */
    PASSWORD_OVERRIDE("password"),

    /**
     * Allows a user to inject a custom HTTP header into the client which overrides or
     * augments the values that the client would use to authenticate with the remote over
     * a WebSocket based connection.
     * <p>
     * Using this method overrides the effect of URI/ConnectionFactory configuration relating
     * to the HTTP headers provided to the remote for authentication.  This method will be invoked
     * on each connection authentication attempt in the presence of a failover configuration.
     * <p>
     * The extension function takes the form of a BiFunction defined as the following:
     * <ul>
     *   <li>
     *     {@link BiFunction}&lt;{@link Connection}, {@link URI}, {@link Map}&gt;
     *   </li>
     * </ul>
     */
    HTTP_HEADERS_OVERRIDE("httpHeaders");

    private final String extensionKey;

    private JmsConnectionExtensions(String key) {
        this.extensionKey = key;
    }

    @Override
    public String toString() {
        return extensionKey;
    }

    public static JmsConnectionExtensions fromString(String extensionName) {
        for (JmsConnectionExtensions ext : JmsConnectionExtensions.values()) {
            if (ext.extensionKey.equalsIgnoreCase(extensionName)) {
                return ext;
            } else if (ext.toString().equalsIgnoreCase(extensionName)) {
                return ext;
            }
        }

        throw new IllegalArgumentException("No Extension with name " + extensionName + " found");
    }
}
