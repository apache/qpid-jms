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
package org.apache.qpid.jms;

import static org.junit.Assert.fail;

import javax.jms.InvalidDestinationException;
import javax.jms.Message;

import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality around JmsConnection
 */
public class JmsMessageProducerTest {

    private static final Logger LOG = LoggerFactory.getLogger(JmsMessageProducerTest.class);

    private JmsConnection connection;
    private  JmsSession session;
    private JmsProducerId producerId;

    @Before
    public void setUp() throws Exception {
        connection = Mockito.mock(JmsConnection.class);
        session = Mockito.mock(JmsSession.class);
        producerId = new JmsProducerId("key");

        Mockito.when(session.getConnection()).thenReturn(connection);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                LOG.debug("Handling connection createResource call");
                if (args[0] instanceof JmsProducerInfo) {
                    return args[0];
                }

                throw new IllegalArgumentException("Not implemented");
            }
        }).when(connection).createResource(Mockito.any(JmsResource.class));
    }

    @Test
    public void testAnonymousProducerThrowsUOEWhenExplictDestinationNotProvided() throws Exception {
        JmsMessageProducer producer = new JmsMessageProducer(producerId, session, null);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(message);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test
    public void testExplicitProducerThrowsUOEWhenExplictDestinationIsProvided() throws Exception {
        JmsDestination dest = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = new JmsMessageProducer(producerId, session, dest);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(dest, message);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(dest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test
    public void testAnonymousDestinationProducerThrowsIDEWhenNullDestinationIsProvided() throws Exception {
        JmsMessageProducer producer = new JmsMessageProducer(producerId, session, null);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(null, message);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }
}
