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
package org.apache.qpid.jms.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.apache.qpid.jms.message.foreign.ForeignJmsBytesMessage;
import org.apache.qpid.jms.message.foreign.ForeignJmsMapMessage;
import org.apache.qpid.jms.message.foreign.ForeignJmsMessage;
import org.apache.qpid.jms.message.foreign.ForeignJmsObjectMessage;
import org.apache.qpid.jms.message.foreign.ForeignJmsStreamMessage;
import org.apache.qpid.jms.message.foreign.ForeignJmsTextMessage;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test Transformation class used to handle foreign JMS Destinations and Messages.
 */
public class JmsMessageTransformationTest {

    private static final String DESTINATION_NAME = "Test-Destination-Name";

    //---------- Test Message Transformation ---------------------------------//

    @Test
    public void testTransformJmsMessage() throws JMSException {
        JmsMessage orig = new JmsMessage(new JmsTestMessageFacade());

        orig.setJMSMessageID("ID:CONNECTION:1:1");

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), orig);
        assertNotNull(transformed.getJMSMessageID());
        assertEquals(orig, transformed);
        assertNotSame(orig, transformed);
    }

    @Test
    public void testForeignMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsMessage foreignMessage = new ForeignJmsMessage();

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));
    }

    @Test
    public void testEmptyForeignBytesMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsBytesMessage foreignMessage = new ForeignJmsBytesMessage();

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsBytesMessage);
        JmsBytesMessage message = (JmsBytesMessage) transformed;
        message.reset();
        assertEquals(0, message.getBodyLength());
    }

    @Test
    public void testForeignBytesMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsBytesMessage foreignMessage = new ForeignJmsBytesMessage();
        foreignMessage.writeBoolean(true);
        foreignMessage.setBooleanProperty("boolProperty", true);

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsBytesMessage);
        JmsBytesMessage message = (JmsBytesMessage) transformed;
        message.reset();
        assertTrue(message.getBodyLength() > 0);
        assertTrue(message.propertyExists("boolProperty"));
    }

    @Test
    public void testEmptyForeignTextMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsTextMessage foreignMessage = new ForeignJmsTextMessage();

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsTextMessage);
        JmsTextMessage message = (JmsTextMessage) transformed;
        assertNull(message.getText());
    }

    @Test
    public void testForeignTextMessageTransformCreateNewMessage() throws JMSException {
        final String MESSAGE_BODY = "TEST-MESSAGE-BODY";

        ForeignJmsTextMessage foreignMessage = new ForeignJmsTextMessage();
        foreignMessage.setText(MESSAGE_BODY);
        foreignMessage.setBooleanProperty("boolProperty", true);

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsTextMessage);
        JmsTextMessage message = (JmsTextMessage) transformed;
        assertEquals(MESSAGE_BODY, message.getText());
        assertTrue(message.propertyExists("boolProperty"));
    }

    @Test
    public void testEmptyForeignMapMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsMapMessage foreignMessage = new ForeignJmsMapMessage();

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsMapMessage);
        JmsMapMessage message = (JmsMapMessage) transformed;
        assertFalse(message.getMapNames().hasMoreElements());
    }

    @Test
    public void testForeignMapMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsMapMessage foreignMessage = new ForeignJmsMapMessage();
        foreignMessage.setBoolean("property1", true);
        foreignMessage.setShort("property2", (short) 65535);
        foreignMessage.setBooleanProperty("boolProperty", true);

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsMapMessage);
        JmsMapMessage message = (JmsMapMessage) transformed;
        assertTrue(message.propertyExists("boolProperty"));
        assertTrue(message.getMapNames().hasMoreElements());
        assertTrue(message.itemExists("property1"));
        assertTrue(message.itemExists("property2"));
    }

    @Test
    public void testEmptyForeignStreamMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsStreamMessage foreignMessage = new ForeignJmsStreamMessage();

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsStreamMessage);
        JmsStreamMessage message = (JmsStreamMessage) transformed;
        message.reset();
        try {
            message.readBoolean();
        } catch (MessageEOFException ex) {}
    }

    @Test
    public void tesAbnormalForeignStreamMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsStreamMessage foreignMessage = new ForeignJmsStreamMessage();
        foreignMessage.writeObject(true);
        foreignMessage.reset();
        foreignMessage = Mockito.spy(foreignMessage);

        // Test for an odd StreamMessage that return null instead of throwing a MessageEOFException
        Mockito.when(foreignMessage.readObject()).thenReturn(true).
                                                  thenReturn(false).
                                                  thenReturn(true).
                                                  thenReturn(null);

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsStreamMessage);
        JmsStreamMessage message = (JmsStreamMessage) transformed;
        message.reset();

        assertTrue(message.readBoolean());
        assertFalse(message.readBoolean());
        assertTrue(message.readBoolean());
        try {
            message.readBoolean();
        } catch (MessageEOFException ex) {}
    }

    @Test
    public void testForeignStreamMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsStreamMessage foreignMessage = new ForeignJmsStreamMessage();
        foreignMessage.writeBoolean(true);
        foreignMessage.writeString("test");
        foreignMessage.writeBoolean(true);
        foreignMessage.setBooleanProperty("boolProperty", true);

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsStreamMessage);
        JmsStreamMessage message = (JmsStreamMessage) transformed;
        assertTrue(message.propertyExists("boolProperty"));
        message.reset();
        assertTrue(message.readBoolean());
        assertEquals("test", message.readString());
        assertTrue(message.readBoolean());
    }

    @Test
    public void testEmptyForeignObjectMessageTransformCreateNewMessage() throws JMSException {
        ForeignJmsObjectMessage foreignMessage = new ForeignJmsObjectMessage();

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsObjectMessage);
        JmsObjectMessage message = (JmsObjectMessage) transformed;
        assertNull(message.getObject());
    }

    @Test
    public void testForeignObjectMessageTransformCreateNewMessage() throws JMSException {
        final String MESSAGE_BODY = "TEST-MESSAGE-BODY";

        ForeignJmsObjectMessage foreignMessage = new ForeignJmsObjectMessage();
        foreignMessage.setBooleanProperty("boolProperty", true);
        foreignMessage.setObject(MESSAGE_BODY);

        JmsMessage transformed = JmsMessageTransformation.transformMessage(createMockJmsConnection(), foreignMessage);
        assertNotSame(foreignMessage, transformed);
        assertFalse(transformed.equals(foreignMessage));

        assertTrue(transformed instanceof JmsObjectMessage);
        JmsObjectMessage message = (JmsObjectMessage) transformed;
        assertTrue(message.propertyExists("boolProperty"));
        assertEquals(MESSAGE_BODY, message.getObject());
    }

    //---------- Test Generic Property Copy ----------------------------------//

    @Test
    public void testJMSMessageHeadersAreCopied() throws JMSException {
        JmsMessage source = new JmsMessage(new JmsTestMessageFacade());
        JmsMessage target = new JmsMessage(new JmsTestMessageFacade());

        JmsTopic destination = new JmsTopic(DESTINATION_NAME);
        JmsTopic replyTo = new JmsTopic("ReplyTp:" + DESTINATION_NAME);

        source.setJMSMessageID("ID:TEST");
        source.setJMSCorrelationID("ID:CORRELATION");
        source.setJMSReplyTo(replyTo);
        source.setJMSDestination(destination);
        source.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        source.setJMSDeliveryTime(10000);
        source.setJMSRedelivered(true);
        source.setJMSType("test-type");
        source.setJMSExpiration(15000);
        source.setJMSPriority(7);
        source.setJMSTimestamp(5000);

        JmsMessageTransformation.copyProperties(createMockJmsConnection(), source, target);

        assertEquals("ID:TEST" , target.getJMSMessageID());
        assertEquals("ID:CORRELATION", target.getJMSCorrelationID());
        assertEquals(replyTo, target.getJMSReplyTo());
        assertEquals(destination, target.getJMSDestination());
        assertEquals(DeliveryMode.NON_PERSISTENT, target.getJMSDeliveryMode());
        assertEquals(10000, target.getJMSDeliveryTime());
        assertEquals(true, target.getJMSRedelivered());
        assertEquals("test-type", target.getJMSType());
        assertEquals(15000, target.getJMSExpiration());
        assertEquals(7, target.getJMSPriority());
        assertEquals(5000, target.getJMSTimestamp());
    }

    @Test
    public void testJMSMessagePropertiesAreCopied() throws JMSException {
        JmsMessage source = new JmsMessage(new JmsTestMessageFacade());
        JmsMessage target = new JmsMessage(new JmsTestMessageFacade());

        source.setJMSType("text/test");

        source.setBooleanProperty("boolValue", true);
        source.setStringProperty("stringValue", "foo");

        JmsMessageTransformation.copyProperties(createMockJmsConnection(), source, target);

        assertEquals(true, target.getBooleanProperty("boolValue"));
        assertEquals("foo", target.getStringProperty("stringValue"));
        assertEquals("text/test", target.getJMSType());
    }

    //---------- Test Destination Transformation -----------------------------//

    @Test
    public void testTransformNullDestinationNoExceptions() throws JMSException {
        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), null);
        assertNull(transformed);
    }

    @Test
    public void testUnresolvedDestinationTransformerGetReturnsNonNull() throws JMSException {
        assertNotNull(JmsMessageTransformation.getUnresolvedDestinationTransformer());
        JmsMessageTransformation.setUnresolvedDestinationHandler(null);
        assertNotNull(JmsMessageTransformation.getUnresolvedDestinationTransformer());
    }

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
    public void testPlainDestinationWithCustomInterceper() throws JMSException {
        ForeignDestination destination = new ForeignDestination(DESTINATION_NAME);
        JmsMessageTransformation.setUnresolvedDestinationHandler(new AlwaysQueueUnresolvedDestinationHandler());
        JmsDestination result = JmsMessageTransformation.transformDestination(createMockJmsConnection(), destination);

        assertNotNull(result);
        assertTrue(result instanceof JmsQueue);
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
    public void testCompositeTopicAndQueueDestinationCanBeInterceper() throws JMSException {
        ForeignDestination destination = new ForeignTopicAndQueue(DESTINATION_NAME);
        JmsMessageTransformation.setUnresolvedDestinationHandler(new AlwaysQueueUnresolvedDestinationHandler());
        JmsDestination result = JmsMessageTransformation.transformDestination(createMockJmsConnection(), destination);

        assertNotNull(result);
        assertTrue(result instanceof JmsQueue);
    }

    @Test
    public void testCompositeTopicAndQueueDestinationNoNameThrowsJMSEx() throws JMSException {
        ForeignTopicAndQueue destination = new ForeignTopicAndQueue(DESTINATION_NAME);
        destination.setReturnQueueName(false);
        destination.setReturnTopicName(false);

        try {
            JmsMessageTransformation.transformDestination(createMockJmsConnection(), destination);
            fail("Should have thrown an JMSException");
        } catch (JMSException ex) {
        }
    }

    @Test
    public void testTransformCompositeDestinationFromForeignTopic() throws JMSException {
        ForeignTopicAndQueue destination = new ForeignTopicAndQueue(DESTINATION_NAME);
        destination.setReturnQueueName(false);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), destination);
        assertNotNull(transformed);
        assertTrue(transformed.isTopic());
        assertFalse(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getAddress());
    }

    @Test
    public void testTransformCompositeDestinationFromForeignQueue() throws JMSException {
        ForeignTopicAndQueue destination = new ForeignTopicAndQueue(DESTINATION_NAME);
        destination.setReturnTopicName(false);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), destination);
        assertNotNull(transformed);
        assertTrue(transformed.isQueue());
        assertFalse(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getAddress());
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
        assertEquals(DESTINATION_NAME, transformed.getAddress());
    }

    @Test
    public void testTransformDestinationFromForeignQueue() throws JMSException {
        ForeignDestination foreignDestination = new ForeignQueue(DESTINATION_NAME);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), foreignDestination);
        assertNotNull(transformed);
        assertTrue(transformed.isQueue());
        assertFalse(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getAddress());
    }

    @Test
    public void testTransformDestinationFromForeignTempQueue() throws JMSException {
        ForeignDestination foreignDestination = new ForeignTemporaryQueue(DESTINATION_NAME);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), foreignDestination);
        assertNotNull(transformed);
        assertTrue(transformed.isQueue());
        assertTrue(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getAddress());
    }

    @Test
    public void testTransformDestinationFromForeignTempTopic() throws JMSException {
        ForeignDestination foreignDestination = new ForeignTemporaryTopic(DESTINATION_NAME);

        JmsDestination transformed = JmsMessageTransformation.transformDestination(createMockJmsConnection(), foreignDestination);
        assertNotNull(transformed);
        assertTrue(transformed.isTopic());
        assertTrue(transformed.isTemporary());
        assertEquals(DESTINATION_NAME, transformed.getAddress());
    }

    //---------- Mocking support ---------------------------------------------//

    private JmsConnection createMockJmsConnection() {
        JmsConnection connection = Mockito.mock(JmsConnection.class);

        Mockito.when(connection.getMessageFactory()).thenReturn(new JmsTestMessageFactory());

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

    private class ForeignTopicAndQueue extends ForeignDestination implements Queue, Topic {

        private boolean returnTopicName = true;
        private boolean returnQueueName = true;

        public ForeignTopicAndQueue(String name) {
            super(name);
        }

        @Override
        public String getTopicName() throws JMSException {
            if (returnTopicName) {
                return name;
            }

            return null;
        }

        @Override
        public String getQueueName() throws JMSException {
            if (returnQueueName) {
                return name;
            }

            return null;
        }

        public void setReturnTopicName(boolean returnTopicName) {
            this.returnTopicName = returnTopicName;
        }

        public void setReturnQueueName(boolean returnQueueName) {
            this.returnQueueName = returnQueueName;
        }
    }

    private class AlwaysQueueUnresolvedDestinationHandler implements JmsUnresolvedDestinationTransformer {

        @Override
        public JmsDestination transform(Destination destination) throws JMSException {
            return new JmsQueue(destination.toString());
        }

        @Override
        public JmsDestination transform(String destination) throws JMSException {
            return new JmsQueue(destination);
        }
    }
}
