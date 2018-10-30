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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.mock.MockRemotePeer;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.After;
import org.junit.Before;

/**
 * Utility methods useful in testing the FailoverProvider
 */
public class FailoverProviderTestSupport extends QpidJmsTestCase {

    private final IdGenerator connectionIdGenerator = new IdGenerator();
    private final AtomicLong nextSessionId = new AtomicLong();
    private final AtomicLong nextConsumerId = new AtomicLong();

    protected MockRemotePeer mockPeer;

    @Override
    @Before
    public void setUp() throws Exception {
        nextSessionId.set(0);
        nextConsumerId.set(0);

        mockPeer = new MockRemotePeer();
        mockPeer.start();

        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (mockPeer != null) {
            mockPeer.terminate();
            mockPeer = null;
        }

        super.tearDown();
    }

    protected JmsConnectionInfo createConnectionInfo() {
        JmsConnectionId id = new JmsConnectionId(connectionIdGenerator.generateId());
        JmsConnectionInfo info = new JmsConnectionInfo(id);
        return info;
    }

    protected JmsSessionInfo createSessionInfo(JmsConnectionInfo connection) {
        JmsSessionId id = new JmsSessionId(connection.getId(), nextSessionId.incrementAndGet());
        JmsSessionInfo info = new JmsSessionInfo(id);
        return info;
    }

    protected JmsConsumerInfo createConsumerInfo(JmsSessionInfo session) {
        JmsConsumerId id = new JmsConsumerId(session.getId(), nextConsumerId.incrementAndGet());
        JmsConsumerInfo consumer = new JmsConsumerInfo(id, null);
        return consumer;
    }
}
