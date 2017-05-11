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
package org.apache.qpid.jms.meta;

import org.apache.qpid.jms.JmsTemporaryTopic;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class JmsDefaultResourceVisitorTest {

    private JmsConnectionId connectionId;
    private JmsSessionId sessionId;
    private JmsProducerId producerId;
    private JmsConsumerId consumerId;
    private JmsTransactionId transactionId;

    @Before
    public void setUp() {
        IdGenerator generator = new IdGenerator();

        connectionId = new JmsConnectionId(generator.generateId());
        sessionId = new JmsSessionId(connectionId, 1);
        producerId = new JmsProducerId(sessionId, 1);
        consumerId = new JmsConsumerId(sessionId, 1);
        transactionId = new JmsTransactionId(connectionId, 0);
    }

    @Test
    public void test() throws Exception {
        JmsDefaultResourceVisitor visitor = new JmsDefaultResourceVisitor();
        visitor.processConnectionInfo(new JmsConnectionInfo(connectionId));
        visitor.processSessionInfo(new JmsSessionInfo(sessionId));
        visitor.processConsumerInfo(new JmsConsumerInfo(consumerId, null));
        visitor.processProducerInfo(new JmsProducerInfo(producerId));
        visitor.processDestination(new JmsTemporaryTopic("Test"));
        visitor.processTransactionInfo(new JmsTransactionInfo(sessionId, transactionId));
    }
}
