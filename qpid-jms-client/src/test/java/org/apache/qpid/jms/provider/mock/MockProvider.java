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
package org.apache.qpid.jms.provider.mock;

import java.io.IOException;
import java.net.URI;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.message.facade.defaults.JmsDefaultMessageFactory;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderListener;

/**
 * Mock Provider instance used in testing higher level classes that interact
 * with a given Provider instance.
 */
public class MockProvider implements Provider {

    private final JmsMessageFactory messageFactory = new JmsDefaultMessageFactory();
    private final MockProviderStats stats;
    private final URI remoteURI;
    private final MockProviderConfiguration configuration;

    private MockProviderListener eventListener;
    private ProviderListener listener;

    public MockProvider(URI remoteURI, MockProviderConfiguration configuration, MockProviderStats global) {
        this.remoteURI = remoteURI;
        this.configuration = configuration;
        this.stats = new MockProviderStats(global);
    }

    @Override
    public void connect() throws IOException {
        stats.recordConnectAttempt();

        if (configuration.isFailOnConnect()) {
            throw new IOException("Failed to connect to: " + remoteURI);
        }
    }

    @Override
    public void start() throws IOException, IllegalStateException {

        if (listener == null) {
            throw new IllegalStateException("Must set a provider listener prior to calling start.");
        }

        if (configuration.isFailOnStart()) {
            throw new IOException();
        }
    }

    @Override
    public void close() {
        stats.recordCloseAttempt();

        if (configuration.isFailOnClose()) {
            throw new RuntimeException();
        }
    }

    @Override
    public URI getRemoteURI() {
        return remoteURI;
    }

    @Override
    public void create(JmsResource resource, AsyncResult request) throws IOException, JMSException {
        stats.recordCreateResourceCall();

        if (resource instanceof JmsConnectionInfo) {
            if (listener != null) {
                listener.onConnectionEstablished(remoteURI);
            }
        }

        request.onSuccess();
    }

    @Override
    public void start(JmsResource resource, AsyncResult request) throws IOException, JMSException {
        stats.recordStartResourceCall();
        request.onSuccess();
    }

    @Override
    public void stop(JmsResource resource, AsyncResult request) throws IOException, JMSException {
        stats.recordStopResourceCall();
        request.onSuccess();
    }

    @Override
    public void destroy(JmsResource resourceId, AsyncResult request) throws IOException, JMSException {
        stats.recordDetroyResourceCall();
        request.onSuccess();
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        stats.recordSendCall();
        request.onSuccess();
    }

    @Override
    public void acknowledge(JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException {
        stats.recoordSessionAcknowledgeCall();
        request.onSuccess();
    }

    @Override
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, AsyncResult request) throws IOException, JMSException {
        stats.recoordAcknowledgeCall();
        request.onSuccess();
    }

    @Override
    public void commit(JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException {
        stats.recordCommitCall();
        request.onSuccess();
    }

    @Override
    public void rollback(JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException {
        stats.recordRollbackCall();
        request.onSuccess();
    }

    @Override
    public void recover(JmsSessionId sessionId, AsyncResult request) throws IOException {
        stats.recordRecoverCall();
        request.onSuccess();
    }

    @Override
    public void unsubscribe(String subscription, AsyncResult request) throws IOException, JMSException {
        stats.recordUnsubscribeCall();
        request.onSuccess();
    }

    @Override
    public void pull(JmsConsumerId consumerId, long timeout, AsyncResult request) throws IOException {
        stats.recordPullCall();
        request.onSuccess();
    }

    @Override
    public JmsMessageFactory getMessageFactory() {
        return messageFactory;
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        if (eventListener != null) {
            eventListener.whenProviderListenerSet(this, listener);
        }
        this.listener = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return listener;
    }

    public MockProvider setEventListener(MockProviderListener eventListener) {
        this.eventListener = eventListener;
        return this;
    }

    public MockProviderConfiguration getConfiguration() {
        return configuration;
    }

    public MockProviderStats getStatistics() {
        return stats;
    }
}
