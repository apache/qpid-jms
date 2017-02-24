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
package org.apache.qpid.jms.provider.amqp.builders;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SOLE_CONNECTION_CAPABILITY;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Session;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.jms.provider.amqp.AmqpRedirect;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.util.MetaDataSupport;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.engine.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource builder responsible for creating and opening an AmqpConnection instance.
 */
public class AmqpConnectionBuilder extends AmqpResourceBuilder<AmqpConnection, AmqpProvider, JmsConnectionInfo, Connection> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnectionBuilder.class);

    public AmqpConnectionBuilder(AmqpProvider parent, JmsConnectionInfo resourceInfo) {
        super(parent, resourceInfo);
    }

    @Override
    public void buildResource(final AsyncResult request) {
        super.buildResource(createRequestIntercepter(request));
    }

    protected AsyncResult createRequestIntercepter(final AsyncResult request) {
        return new AsyncResult() {

            @Override
            public void onSuccess() {
                // Create a Session for this connection that is used for Temporary Destinations
                // and perhaps later on management and advisory monitoring.
                JmsSessionInfo sessionInfo = new JmsSessionInfo(getResourceInfo(), -1);
                sessionInfo.setAcknowledgementMode(Session.AUTO_ACKNOWLEDGE);

                final AmqpConnectionSessionBuilder builder = new AmqpConnectionSessionBuilder(getResource(), sessionInfo);
                builder.buildResource(new AsyncResult() {

                    @Override
                    public boolean isComplete() {
                        return builder.getResource().isOpen();
                    }

                    @Override
                    public void onSuccess() {
                        LOG.debug("{} is now open: ", getResource());
                        request.onSuccess();
                    }

                    @Override
                    public void onFailure(Throwable result) {
                        LOG.debug("AMQP Connection Session failed to open.");
                        request.onFailure(result);
                    }
                });
            }

            @Override
            public void onFailure(Throwable result) {
                request.onFailure(result);
            }

            @Override
            public boolean isComplete() {
                return getResource().isOpen();
            }
        };
    }

    @Override
    protected Connection createEndpoint(JmsConnectionInfo resourceInfo) {
        String hostname = getParent().getVhost();
        if (hostname == null) {
            hostname = getParent().getRemoteURI().getHost();
        } else if (hostname.isEmpty()) {
            hostname = null;
        }

        Map<Symbol, Object> props = new LinkedHashMap<Symbol, Object>();
        props.put(AmqpSupport.PRODUCT, MetaDataSupport.PROVIDER_NAME);
        props.put(AmqpSupport.VERSION, MetaDataSupport.PROVIDER_VERSION);
        props.put(AmqpSupport.PLATFORM, MetaDataSupport.PLATFORM_DETAILS);

        Connection connection = getParent().getProtonConnection();
        connection.setHostname(hostname);
        connection.setContainer(resourceInfo.getClientId());
        connection.setDesiredCapabilities(new Symbol[] { SOLE_CONNECTION_CAPABILITY });
        connection.setProperties(props);

        return connection;
    }

    @Override
    protected AmqpConnection createResource(AmqpProvider parent, JmsConnectionInfo resourceInfo, Connection endpoint) {
        return new AmqpConnection(parent, resourceInfo, endpoint);
    }

    @Override
    protected void afterOpened() {
        // Initialize the connection properties so that the state of the remote can
        // be determined, this allows us to check for close pending.
        getResource().getProperties().initialize(
            getEndpoint().getRemoteOfferedCapabilities(), getEndpoint().getRemoteProperties());

        // If there are failover servers in the open then we signal that to the listeners
        List<AmqpRedirect> failoverList = getResource().getProperties().getFailoverServerList();
        if (!failoverList.isEmpty()) {
            List<URI> failoverURIs = new ArrayList<>();
            for (AmqpRedirect redirect : failoverList) {
                try {
                    failoverURIs.add(redirect.toURI());
                } catch (Exception ex) {
                    LOG.trace("Error while creating URI from failover server: {}", redirect);
                }
            }

            if (!failoverURIs.isEmpty()) {
                getResource().getProvider().fireRemotesDiscovered(failoverURIs);
            }
        }
    }

    @Override
    protected boolean isClosePending() {
        return getResource().getProperties().isConnectionOpenFailed();
    }

    @Override
    protected long getRequestTimeout() {
        return getParent().getProvider().getConnectTimeout();
    }
}
