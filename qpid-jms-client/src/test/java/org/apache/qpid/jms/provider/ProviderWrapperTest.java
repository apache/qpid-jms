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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.net.URI;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;
import org.mockito.Mockito;

public class ProviderWrapperTest extends QpidJmsTestCase{

    @Test
    public void testGetMessageFactory() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.getMessageFactory();
        Mockito.verify(mockProvider).getMessageFactory();
    }

    @Test
    public void testGetNext() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        assertSame(mockProvider, wrapper.getNext());
    }

    @Test
    public void testAcknowledgeMessage() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        JmsInboundMessageDispatch inbound = Mockito.mock(JmsInboundMessageDispatch.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.acknowledge(inbound, ACK_TYPE.ACCEPTED, result);
        Mockito.verify(mockProvider).acknowledge(inbound, ACK_TYPE.ACCEPTED, result);
    }

    @Test
    public void testAcknowledgeSession() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.acknowledge(sessionId, ACK_TYPE.ACCEPTED, result);
        Mockito.verify(mockProvider).acknowledge(sessionId, ACK_TYPE.ACCEPTED, result);
    }

    @Test
    public void testRecover() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.recover(sessionId, result);
        Mockito.verify(mockProvider).recover(sessionId, result);
    }

    @Test
    public void testStart() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        try {
            wrapper.start();
            fail("should have thrown exception due to no listener");
        } catch (IllegalStateException ise) {
        }

        ProviderListener listener = Mockito.mock(ProviderListener.class);

        wrapper.setProviderListener(listener);
        wrapper.start();

        assertSame(listener, wrapper.getProviderListener());

        Mockito.verify(mockProvider).start();
    }

    @Test
    public void testClose() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.close();
        Mockito.verify(mockProvider).close();
    }

    @Test
    public void testConnect() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        JmsConnectionId id = new JmsConnectionId("ID:1");
        JmsConnectionInfo connectionInfo = new JmsConnectionInfo(id);

        wrapper.connect(connectionInfo);
        Mockito.verify(mockProvider).connect(connectionInfo);
    }

    @Test
    public void testGetRemoteURI() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.getRemoteURI();
        Mockito.verify(mockProvider).getRemoteURI();
    }

    @Test
    public void testCreate() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);
        JmsSessionInfo session = new JmsSessionInfo(sessionId);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.create(session, result);
        Mockito.verify(mockProvider).create(session, result);
    }

    @Test
    public void testStartResource() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);
        JmsSessionInfo session = new JmsSessionInfo(sessionId);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.start(session, result);
        Mockito.verify(mockProvider).start(session, result);
    }

    @Test
    public void testStopResource() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);
        JmsSessionInfo session = new JmsSessionInfo(sessionId);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.stop(session, result);
        Mockito.verify(mockProvider).stop(session, result);
    }

    @Test
    public void testDestroy() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);
        JmsSessionInfo session = new JmsSessionInfo(sessionId);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.destroy(session, result);
        Mockito.verify(mockProvider).destroy(session, result);
    }

    @Test
    public void testSend() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        JmsOutboundMessageDispatch envelope = Mockito.mock(JmsOutboundMessageDispatch.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.send(envelope, result);
        Mockito.verify(mockProvider).send(envelope, result);
    }

    @Test
    public void testUnsubscribe() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        String subscriptionName = "subName";

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.unsubscribe(subscriptionName, result);
        Mockito.verify(mockProvider).unsubscribe(subscriptionName, result);
    }

    @Test
    public void testCommit() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsConnectionId connectionId = new JmsConnectionId("ID");
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);
        JmsTransactionId txId = new JmsTransactionId(connectionId, 1);
        JmsTransactionInfo txInfo = new JmsTransactionInfo(sessionId, txId);
        JmsTransactionId nextTxId = new JmsTransactionId(connectionId, 2);
        JmsTransactionInfo nextTxInfo = new JmsTransactionInfo(sessionId, nextTxId);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.commit(txInfo, nextTxInfo, result);
        Mockito.verify(mockProvider).commit(txInfo, nextTxInfo, result);
    }

    @Test
    public void testRollback() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsConnectionId connectionId = new JmsConnectionId("ID");
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);
        JmsTransactionId txId = new JmsTransactionId(connectionId, 1);
        JmsTransactionInfo txInfo = new JmsTransactionInfo(sessionId, txId);
        JmsTransactionId nextTxId = new JmsTransactionId(connectionId, 2);
        JmsTransactionInfo nextTxInfo = new JmsTransactionInfo(sessionId, nextTxId);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.rollback(txInfo, nextTxInfo, result);
        Mockito.verify(mockProvider).rollback(txInfo, nextTxInfo, result);
    }

    @Test
    public void testPull() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        AsyncResult result = Mockito.mock(AsyncResult.class);
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);
        JmsConsumerId consumerId = new JmsConsumerId(sessionId, 1);

        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);

        wrapper.pull(consumerId, 1000, result);
        Mockito.verify(mockProvider).pull(consumerId, 1000, result);
    }

    @Test
    public void testOnInboundMessage() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        JmsInboundMessageDispatch envelope = Mockito.mock(JmsInboundMessageDispatch.class);
        ProviderListener listener = Mockito.mock(ProviderListener.class);

        wrapper.setProviderListener(listener);
        wrapper.onInboundMessage(envelope);

        Mockito.verify(listener).onInboundMessage(envelope);
    }

    @Test
    public void testOnCompletedMessageSend() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        JmsOutboundMessageDispatch envelope = Mockito.mock(JmsOutboundMessageDispatch.class);
        ProviderListener listener = Mockito.mock(ProviderListener.class);

        wrapper.setProviderListener(listener);
        wrapper.onCompletedMessageSend(envelope);

        Mockito.verify(listener).onCompletedMessageSend(envelope);
    }

    @Test
    public void testOnFailedMessageSend() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        JmsOutboundMessageDispatch envelope = Mockito.mock(JmsOutboundMessageDispatch.class);
        ProviderListener listener = Mockito.mock(ProviderListener.class);
        ProviderException ex = new ProviderException("Error");

        wrapper.setProviderListener(listener);
        wrapper.onFailedMessageSend(envelope, ex);

        Mockito.verify(listener).onFailedMessageSend(envelope, ex);
    }

    @Test
    public void testOnConnectionInterrupted() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        ProviderListener listener = Mockito.mock(ProviderListener.class);
        URI remoteURI = new URI("tcp://localhost");

        wrapper.setProviderListener(listener);
        wrapper.onConnectionInterrupted(remoteURI);

        Mockito.verify(listener).onConnectionInterrupted(remoteURI);
    }

    @Test
    public void testOnConnectionEstablished() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        ProviderListener listener = Mockito.mock(ProviderListener.class);
        URI remoteURI = new URI("tcp://localhost");
        Mockito.when(mockProvider.getRemoteURI()).thenReturn(remoteURI);

        wrapper.setProviderListener(listener);
        wrapper.onConnectionEstablished(remoteURI);

        Mockito.verify(listener).onConnectionEstablished(remoteURI);
    }

    @Test
    public void testOnConnectionRestored() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        ProviderListener listener = Mockito.mock(ProviderListener.class);
        URI remoteURI = new URI("tcp://localhost");

        wrapper.setProviderListener(listener);
        wrapper.onConnectionRestored(remoteURI);

        Mockito.verify(listener).onConnectionRestored(remoteURI);
    }

    @Test
    public void testOnResourceClosed() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        ProviderListener listener = Mockito.mock(ProviderListener.class);
        JmsSessionId sessionId = new JmsSessionId("ID:TEST", 1);
        JmsSessionInfo session = new JmsSessionInfo(sessionId);
        ProviderException ex = new ProviderException("Error");

        wrapper.setProviderListener(listener);
        wrapper.onResourceClosed(session, ex);

        Mockito.verify(listener).onResourceClosed(session, ex);
    }

    @Test
    public void testOnProviderException() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        ProviderListener listener = Mockito.mock(ProviderListener.class);
        ProviderException ex = new ProviderException("Error");

        wrapper.setProviderListener(listener);
        wrapper.onProviderException(ex);

        Mockito.verify(listener).onProviderException(ex);
    }

    @Test
    public void testOnConnectionFailure() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        ProviderListener listener = Mockito.mock(ProviderListener.class);
        ProviderException ex = new ProviderException("Something went wrong");

        wrapper.setProviderListener(listener);
        wrapper.onConnectionFailure(ex);

        Mockito.verify(listener).onConnectionFailure(ex);
    }

    @Test
    public void onConnectionRecovery() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        ProviderListener listener = Mockito.mock(ProviderListener.class);

        wrapper.setProviderListener(listener);
        wrapper.onConnectionRecovery(mockProvider);

        Mockito.verify(listener).onConnectionRecovery(mockProvider);
    }

    @Test
    public void onConnectionRecoverd() throws Exception {
        Provider mockProvider = Mockito.mock(Provider.class);
        ProviderWrapper<Provider> wrapper = new ProviderWrapper<Provider>(mockProvider);
        ProviderListener listener = Mockito.mock(ProviderListener.class);

        wrapper.setProviderListener(listener);
        wrapper.onConnectionRecovered(mockProvider);

        Mockito.verify(listener).onConnectionRecovered(mockProvider);
    }
}
