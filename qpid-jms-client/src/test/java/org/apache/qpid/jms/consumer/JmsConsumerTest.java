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
package org.apache.qpid.jms.consumer;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Map;

import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsConsumer;
import org.apache.qpid.jms.JmsMessageConsumer;
import org.apache.qpid.jms.JmsSession;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * Test for basic behavior of the JMSConsumer implementation.
 */
public class JmsConsumerTest {

    @Test
    public void testGetMessageSelector() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        try {
            assertNull(consumer.getMessageSelector());
        } finally {
            consumer.close();
        }

        Mockito.verify(messageConsumer, Mockito.times(1)).getMessageSelector();
    }

    @Test
    public void testGetMessageListener() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        try {
            assertNull(consumer.getMessageListener());
        } finally {
            consumer.close();
        }

        Mockito.verify(messageConsumer, Mockito.times(1)).getMessageListener();
    }

    @Test
    public void testReceivePassthrough() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        try {
            assertNull(consumer.receive());
        } finally {
            consumer.close();
        }

        Mockito.verify(messageConsumer, Mockito.times(1)).receive();
    }

    @Test
    public void testTimedReceivePassthrough() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        try {
            assertNull(consumer.receive(100));
        } finally {
            consumer.close();
        }

        Mockito.verify(messageConsumer, Mockito.times(1)).receive(Matchers.anyInt());
    }

    @Test
    public void testReceiveNoWaitPassthrough() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        try {
            assertNull(consumer.receiveNoWait());
        } finally {
            consumer.close();
        }

        Mockito.verify(messageConsumer, Mockito.times(1)).receiveNoWait();
    }

    @Test
    public void testReceiveBodyPassthrough() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        try {
            assertNull(consumer.receiveBody(Map.class));
        } finally {
            consumer.close();
        }

        Mockito.verify(messageConsumer, Mockito.times(1)).receiveBody(Map.class, -1);
    }

    @Test
    public void testTimedReceiveBodyPassthrough() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        try {
            assertNull(consumer.receiveBody(Map.class, 100));
        } finally {
            consumer.close();
        }

        Mockito.verify(messageConsumer, Mockito.times(1)).receiveBody(Map.class, 100);
    }

    @Test
    public void testNoWaitReceiveBodyPassthrough() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        try {
            assertNull(consumer.receiveBodyNoWait(Map.class));
        } finally {
            consumer.close();
        }

        Mockito.verify(messageConsumer, Mockito.times(1)).receiveBody(Map.class, 0);
    }

    //----- Test Receive calls map zero to infinite wait ---------------------//

    @Test
    public void testReceiveBodyWithTimeoutZeroWaitsForever() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        consumer.receiveBody(Map.class, 0);
        consumer.close();

        Mockito.verify(messageConsumer).receiveBody(Map.class, -1);
    }

    //----- Test that the JMSRuntimeException retains error context ----------//

    @Test
    public void testRuntimeExceptionOnClose() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).close();

        try {
            consumer.close();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        }
    }

    @Test
    public void testRuntimeExceptionOnGetMessageListener() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).getMessageListener();

        try {
            consumer.getMessageListener();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnSetMessageListener() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).setMessageListener(null);

        try {
            consumer.setMessageListener(null);
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnGetMessageSelector() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).getMessageSelector();

        try {
            consumer.getMessageSelector();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnReceive() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).receive();

        try {
            consumer.receive();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnReceiveNoWait() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).receiveNoWait();

        try {
            consumer.receiveNoWait();
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnTimedReceive() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).receive(Matchers.anyInt());

        try {
            consumer.receive(100);
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnReceiveBody() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).receiveBody(Map.class, -1);

        try {
            consumer.receiveBody(Map.class);
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnTimedReceiveBody() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).receiveBody(Map.class, 100);

        try {
            consumer.receiveBody(Map.class, 100);
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testRuntimeExceptionOnReceiveBodyNoWait() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageConsumer messageConsumer = Mockito.mock(JmsMessageConsumer.class);
        JmsConsumer consumer = new JmsConsumer(session, messageConsumer);

        Mockito.doThrow(IllegalStateException.class).when(messageConsumer).receiveBody(Map.class, 0);

        try {
            consumer.receiveBodyNoWait(Map.class);
            fail("Should throw ISRE");
        } catch (IllegalStateRuntimeException isre) {
        } finally {
            consumer.close();
        }
    }
}
