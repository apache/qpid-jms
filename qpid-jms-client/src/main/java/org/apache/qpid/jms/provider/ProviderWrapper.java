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
package org.apache.qpid.jms.provider;

import java.io.IOException;
import java.net.URI;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;

/**
 * Allows one AsyncProvider instance to wrap around another and provide some additional
 * features beyond the normal AsyncProvider interface.
 *
 * This wrapper is meant primarily for Providers that are adding some additional feature
 * on-top of an existing provider such as a discovery based provider that only needs to
 * pass along discovered remote peer information.
 */
public class ProviderWrapper<E extends Provider> implements Provider, ProviderListener {

    protected final E next;
    protected ProviderListener listener;

    public ProviderWrapper(E next) {
        this.next = next;
        this.next.setProviderListener(this);
    }

    @Override
    public void connect() throws IOException {
        next.connect();
    }

    @Override
    public void start() throws IOException, IllegalStateException {
        if (this.listener == null) {
            throw new IllegalStateException("Cannot start with null ProviderListener");
        }
        next.start();
    }

    @Override
    public void close() {
        next.close();
    }

    @Override
    public URI getRemoteURI() {
        return next.getRemoteURI();
    }

    @Override
    public void create(JmsResource resource, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        next.create(resource, request);
    }

    @Override
    public void start(JmsResource resource, AsyncResult request) throws IOException, JMSException {
        next.start(resource, request);
    }

    @Override
    public void destroy(JmsResource resourceId, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        next.destroy(resourceId, request);
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        next.send(envelope, request);
    }

    @Override
    public void acknowledge(JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException {
        next.acknowledge(sessionId, request);
    }

    @Override
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, AsyncResult request) throws IOException, JMSException {
        next.acknowledge(envelope, ackType, request);
    }

    @Override
    public void commit(JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        next.commit(sessionId, request);
    }

    @Override
    public void rollback(JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        next.rollback(sessionId, request);
    }

    @Override
    public void recover(JmsSessionId sessionId, AsyncResult request) throws IOException, UnsupportedOperationException {
        next.recover(sessionId, request);
    }

    @Override
    public void unsubscribe(String subscription, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        next.unsubscribe(subscription, request);
    }

    @Override
    public void pull(JmsConsumerId consumerId, long timeout, AsyncResult request) throws IOException, UnsupportedOperationException {
        next.pull(consumerId, timeout, request);
    }

    @Override
    public JmsMessageFactory getMessageFactory() {
        return next.getMessageFactory();
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        this.listener = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return this.listener;
    }

    @Override
    public void onMessage(JmsInboundMessageDispatch envelope) {
        this.listener.onMessage(envelope);
    }

    @Override
    public void onConnectionInterrupted(URI remoteURI) {
        this.listener.onConnectionInterrupted(remoteURI);
    }

    @Override
    public void onConnectionRecovery(Provider provider) throws Exception {
        this.listener.onConnectionRecovery(provider);
    }

    @Override
    public void onConnectionRecovered(Provider provider) throws Exception {
        this.listener.onConnectionRecovered(provider);
    }

    @Override
    public void onConnectionRestored(URI remoteURI) {
        this.listener.onConnectionRestored(remoteURI);
    }

    @Override
    public void onConnectionFailure(IOException ex) {
        this.listener.onConnectionInterrupted(this.next.getRemoteURI());
    }

    /**
     * @return the wrapped AsyncProvider.
     */
    public Provider getNext() {
        return this.next;
    }
}
