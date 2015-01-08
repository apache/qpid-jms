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
package org.apache.qpid.jms.transports.vertx;

import java.net.URI;
import java.util.Map;

import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportFactory;
import org.apache.qpid.jms.util.PropertyUtil;

/**
 * Factory for creating the Vert.x based TCP Transport
 */
public class TcpTransportFactory extends TransportFactory {

    @Override
    public Transport createTransport(URI remoteURI) throws Exception {

        Map<String, String> map = PropertyUtil.parseQuery(remoteURI.getQuery());
        Map<String, String> transportURIOptions = PropertyUtil.filterProperties(map, "transport.");

        remoteURI = PropertyUtil.replaceQuery(remoteURI, map);

        TransportOptions transportOptions = new TransportOptions();

        if (!PropertyUtil.setProperties(transportOptions, transportURIOptions)) {
            String msg = " Not all transport options could be set on the Transport." +
                         " Check the options are spelled correctly." +
                         " Given parameters=[" + transportURIOptions + "]." +
                         " This provider instance cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        Transport result = doCreateTransport(remoteURI, transportOptions);

        return result;
    }

    protected TcpTransport doCreateTransport(URI remoteURI, TransportOptions transportOptions) throws Exception {
        return new TcpTransport(remoteURI, transportOptions);
    }

    @Override
    public String getName() {
        return "TCP";
    }
}
