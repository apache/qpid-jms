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
package org.apache.qpid.jms.provider.failover;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.Collections;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderFutureFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Test that methods of FailoverProvider all fail immediately when it is closed.
 */
public class FailoverProviderClosedTest extends FailoverProviderTestSupport {

    private final ProviderFutureFactory futuresFactory = ProviderFutureFactory.create(Collections.emptyMap());

    private FailoverProvider provider;
    private JmsConnectionInfo connection;
    private JmsSessionInfo session;
    private JmsConsumerInfo consumer;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        provider = (FailoverProvider) FailoverProviderFactory.create(new URI("failover:(mock://localhost)"));
        provider.close();

        connection = createConnectionInfo();
        session = createSessionInfo(connection);
        consumer = createConsumerInfo(session);
    }

    @Test
    @Timeout(30)
    public void testMultipleCloseCalls() {
        provider.close();
    }

    @Test
    @Timeout(30)
    public void testConnect() throws Exception {
        assertThrows(ProviderException.class, () -> {
            provider.connect(connection);
        });
    }

    @Test
    @Timeout(30)
    public void testStart() throws Exception {
        assertThrows(ProviderException.class, () -> {
            provider.start();
        });
    }

    @Test
    @Timeout(30)
    public void testCreateResource() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.create(connection, request);
        });
    }

    @Test
    @Timeout(30)
    public void testStartResource() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.start(session, request);
        });
    }

    @Test
    @Timeout(30)
    public void testStopResource() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.stop(session, request);
        });
    }

    @Test
    @Timeout(30)
    public void testDestroyResource() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.destroy(session, request);
        });
    }

    @Test
    @Timeout(30)
    public void testSend() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.send(new JmsOutboundMessageDispatch(), request);
        });
    }

    @Test
    @Timeout(30)
    public void testSessionAcknowledge() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.acknowledge(session.getId(), ACK_TYPE.ACCEPTED, request);
        });
    }

    @Test
    @Timeout(30)
    public void testAcknowledgeMessage() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.acknowledge(new JmsInboundMessageDispatch(1), ACK_TYPE.ACCEPTED, request);
        });
    }

    @Test
    @Timeout(30)
    public void testCommit() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            JmsTransactionId txId = new JmsTransactionId(connection.getId(), 1);
            JmsTransactionInfo txInfo = new JmsTransactionInfo(session.getId(), txId);
            provider.commit(txInfo, null, request);
        });
    }

    @Test
    @Timeout(30)
    public void testRollback() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            JmsTransactionId txId = new JmsTransactionId(connection.getId(), 1);
            JmsTransactionInfo txInfo = new JmsTransactionInfo(session.getId(), txId);
            provider.rollback(txInfo, null, request);
        });
    }

    @Test
    @Timeout(30)
    public void testRecover() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.recover(session.getId(), request);
        });
    }

    @Test
    @Timeout(30)
    public void testUnsubscribe() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.unsubscribe("subscription-name", request);
        });
    }

    @Test
    @Timeout(30)
    public void testMessagePull() throws Exception {
        assertThrows(ProviderException.class, () -> {
            ProviderFuture request = futuresFactory.createFuture();
            provider.pull(consumer.getId(), 1, request);
        });
    }
}
