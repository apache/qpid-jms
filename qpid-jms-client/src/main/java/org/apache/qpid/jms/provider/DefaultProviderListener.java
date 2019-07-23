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
package org.apache.qpid.jms.provider;

import java.net.URI;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsResource;

/**
 * Default implementation that does nothing for all callbacks.
 */
public class DefaultProviderListener implements ProviderListener {

    @Override
    public void onInboundMessage(JmsInboundMessageDispatch envelope) {
    }

    @Override
    public void onCompletedMessageSend(JmsOutboundMessageDispatch envelope) {
    }

    @Override
    public void onFailedMessageSend(JmsOutboundMessageDispatch envelope, ProviderException cause) {
    }

    @Override
    public void onConnectionInterrupted(URI remoteURI) {
    }

    @Override
    public void onConnectionEstablished(URI remoteURI) {
    }

    @Override
    public void onConnectionFailure(ProviderException ex) {
    }

    @Override
    public void onConnectionRecovery(Provider provider) {
    }

    @Override
    public void onConnectionRecovered(Provider provider) {
    }

    @Override
    public void onConnectionRestored(URI remoteURI) {
    }

    @Override
    public void onResourceClosed(JmsResource resource, ProviderException cause) {
    }

    @Override
    public void onProviderException(ProviderException cause) {
    }
}
