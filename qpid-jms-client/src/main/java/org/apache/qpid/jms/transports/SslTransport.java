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

import java.io.IOException;
import java.net.URI;

import org.apache.qpid.jms.JmsSslContext;
import org.vertx.java.core.net.NetClient;

/**
 * Provides SSL configuration to the Vert.x NetClient object used by the underling
 * TCP based Transport.
 */
public class SslTransport extends TcpTransport {

    private final JmsSslContext context;

    /**
     * Create an instance of the SSL transport
     *
     * @param listener
     *        The TransportListener that will handle events from this Transport instance.
     * @param remoteLocation
     *        The location that is being connected to.
     * @param JmsSslContext
     *        The JMS Framework SslContext to use for this SSL connection.
     */
    public SslTransport(TransportListener listener, URI remoteLocation, JmsSslContext context) {
        super(listener, remoteLocation);

        this.context = context;
    }

    @Override
    protected void configureNetClient(NetClient client) throws IOException {
        super.configureNetClient(client);

        client.setSSL(true);
        client.setKeyStorePath(context.getKeyStoreLocation());
        client.setKeyStorePassword(context.getKeyStorePassword());
        client.setTrustStorePath(context.getTrustStoreLocation());
        client.setTrustStorePassword(context.getTrustStorePassword());
    }
}
