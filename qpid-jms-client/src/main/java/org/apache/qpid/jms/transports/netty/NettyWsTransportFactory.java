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
package org.apache.qpid.jms.transports.netty;

import java.net.URI;
import java.util.Map;

import org.apache.qpid.jms.transports.TransportFactory;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.util.PropertyUtil;

/**
 * Factory for creating the Netty based WebSocket Transport.
 */
public class NettyWsTransportFactory extends TransportFactory {

    @Override
    protected NettyTcpTransport doCreateTransport(URI remoteURI, TransportOptions transportOptions) throws Exception {
        return new NettyWsTransport(remoteURI, transportOptions, isSecure());
    }

    @Override
    public String getName() {
        return "WS";
    }

    @Override
    protected TransportOptions applyTransportConfiguration(TransportOptions transportOptions, Map<String, String> transportURIOptions) {
        // WS Transport allows HTTP Headers in its configuration using a prefix "transport.ws.httpHeader.X=Y"
        Map<String, String> httpHeaders =
            PropertyUtil.filterProperties(transportURIOptions, "ws.httpHeader.");

        // We have no way to validate what was configured so apply them all and let the user sort
        // out any resulting mess if the connection should fail.
        transportOptions.getHttpHeaders().putAll(httpHeaders);

        return super.applyTransportConfiguration(transportOptions, transportURIOptions);
    }
}
