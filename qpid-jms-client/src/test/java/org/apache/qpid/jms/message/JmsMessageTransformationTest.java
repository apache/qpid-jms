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
package org.apache.qpid.jms.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsTopic;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test Transformation class used to handle foreign JMS Destinations and Messages.
 */
public class JmsMessageTransformationTest {

    private static final String DESTINATION_NAME = "Test-Destination-Name";

    @Test
    public void testJmsDestinationCreate() throws JMSException {
        new JmsMessageTransformation();
    }

    //---------- Test Destination Transformation -----------------------------//

    @Test
    public void testPlainDestinationThrowsJMSEx() throws JMSException {
        ForeignDestination destination = new ForeignDestination(DESTINATION_NAME);
        try {
            JmsMessageTransformation.transformDestination(createMockJmsConnection(), destination);
            fail("Should have thrown an JMSException");
        } catch (JMSException ex) {
        }
    }

    @Test
    public void testCompositeTopicAndQueueDestinationThrowsJMSEx() throws JMSException {
        ForeignDestination destination = new ForeignTopicAndQueue(DESTINATION_NAME);
        try {
            JmsMessageTransformation.transformDestination(createMockJmsConnection(), destination);
            fail("Should have thrown an JMSException");
        } catch (JMSException ex) {
        }
    }

    @Test
    public void testJmsDestinationIsNotTransformed() throws JMSException {
        JmsDestination destination = new JmsTopic(DESTINATION_NAME);
        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), destination);
        assertSame(destination, transformed);
    }

    @Test
    public void testTransformDestinationFromForeignTopic() throws JMSException {
        ForeignDestination foreignDestination = new ForeignTopic(DESTINATION_NAME);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), foreignDestination);
        assertNotNull(transformed);
        assertTrue(transformed.isTopic());
        assertFalse(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getName());
    }

    @Test
    public void testTransformDestinationFromForeignQueue() throws JMSException {
        ForeignDestination foreignDestination = new ForeignQueue(DESTINATION_NAME);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), foreignDestination);
        assertNotNull(transformed);
        assertTrue(transformed.isQueue());
        assertFalse(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getName());
    }

    @Test
    public void testTransformDestinationFromForeignTempQueue() throws JMSException {
        ForeignDestination foreignDestination = new ForeignTemporaryQueue(DESTINATION_NAME);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), foreignDestination);
        assertNotNull(transformed);
        assertTrue(transformed.isQueue());
        assertTrue(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getName());
    }

    @Test
    public void testTransformDestinationFromForeignTempTopic() throws JMSException {
        ForeignDestination foreignDestination = new ForeignTemporaryTopic(DESTINATION_NAME);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), foreignDestination);
        assertNotNull(transformed);
        assertTrue(transformed.isTopic());
        assertTrue(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getName());
    }

    //---------- Mocking support ---------------------------------------------//

    private JmsConnection createMockJmsConnection() {
        JmsConnection connection = Mockito.mock(JmsConnection.class);

        return connection;
    }

    //---------- Foreign JMS Destinations ------------------------------------//

    private class ForeignDestination implements Destination {

        protected final String name;

        public ForeignDestination(String name) {
            this.name = name;
        }
    }

    private class ForeignTopic extends ForeignDestination implements Topic {

        public ForeignTopic(String name) {
            super(name);
        }

        @Override
        public String getTopicName() throws JMSException {
            return name;
        }
    }

    private class ForeignQueue extends ForeignDestination implements Queue {

        public ForeignQueue(String name) {
            super(name);
        }

        @Override
        public String getQueueName() throws JMSException {
            return name;
        }
    }

    private class ForeignTopicAndQueue extends ForeignDestination implements Queue, Topic {

        public ForeignTopicAndQueue(String name) {
            super(name);
        }

        @Override
        public String getTopicName() throws JMSException {
            return name;
        }

        @Override
        public String getQueueName() throws JMSException {
            return name;
        }
    }

    private class ForeignTemporaryQueue extends ForeignQueue implements TemporaryQueue {

        public ForeignTemporaryQueue(String name) {
            super(name);
        }

        @Override
        public String getQueueName() throws JMSException {
            return name;
        }

        @Override
        public void delete() throws JMSException {

        }
    }

    private class ForeignTemporaryTopic extends ForeignTopic implements TemporaryTopic {

        public ForeignTemporaryTopic(String name) {
            super(name);
        }

        @Override
        public String getTopicName() throws JMSException {
            return name;
        }

        @Override
        public void delete() throws JMSException {
        }
    }
}
