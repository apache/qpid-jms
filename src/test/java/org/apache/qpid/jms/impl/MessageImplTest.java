/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.impl;

import static org.apache.qpid.jms.impl.ClientProperties.JMS_AMQP_TTL;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.TestAmqpMessage;
import org.apache.qpid.proton.amqp.Binary;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MessageImplTest extends QpidJmsTestCase
{
    private ConnectionImpl _mockConnectionImpl;
    private SessionImpl _mockSessionImpl;
    private MessageImpl<TestAmqpMessage> _testMessage;
    private AmqpMessage _testAmqpMessage;
    private String _mockQueueName;
    private Queue _mockQueue;
    private String _mockTopicName;
    private Topic _mockTopic;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _mockConnectionImpl = Mockito.mock(ConnectionImpl.class);
        _mockSessionImpl = Mockito.mock(SessionImpl.class);
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
        Mockito.when(_mockSessionImpl.getMessageIdHelper()).thenReturn(new MessageIdHelper());

        _testAmqpMessage = TestAmqpMessage.createNewMessage();
        _testMessage = TestMessageImpl.createNewMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl);

        _mockQueueName = "mockQueueName";
        _mockQueue = Mockito.mock(Queue.class);
        Mockito.when(_mockQueue.getQueueName()).thenReturn(_mockQueueName);

        _mockTopicName = "mockTopicName";
        _mockTopic = Mockito.mock(Topic.class);
        Mockito.when(_mockTopic.getTopicName()).thenReturn(_mockTopicName);
    }

    @Test
    public void testSetObjectPropertyWithNullOrEmptyNameThrowsIAE() throws Exception
    {
        try
        {
            _testMessage.setObjectProperty(null, "value");
            fail("Expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        try
        {
            _testMessage.setObjectProperty("", "value");
            fail("Expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    @Test
    public void testSetObjectPropertyWithIllegalTypeThrowsMFE() throws Exception
    {
        try
        {
            _testMessage.setObjectProperty("myProperty", new Exception());
            fail("Expected exception not thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    @Test
    public void testSetGetObjectProperty() throws Exception
    {
        String propertyName = "myProperty";

        Object propertyValue = null;
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Boolean.valueOf(false);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Byte.valueOf((byte)1);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Short.valueOf((short)2);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Integer.valueOf(3);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Long.valueOf(4);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Float.valueOf(5.01F);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Double.valueOf(6.01);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = "string";
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));
    }

    @Test
    public void testPropertyExists() throws Exception
    {
        String propertyName = "myProperty";

        assertFalse(_testMessage.propertyExists(propertyName));
        _testMessage.setObjectProperty(propertyName, "string");
        assertTrue(_testMessage.propertyExists(propertyName));
    }

    @Test
    public void testGetPropertyNames() throws Exception
    {
        String propertyName = "myProperty";

        _testMessage.setObjectProperty(propertyName, "string");
        Enumeration<?> names = _testMessage.getPropertyNames();

        assertTrue(names.hasMoreElements());
        Object name1 = names.nextElement();
        assertTrue(name1 instanceof String);
        assertTrue(propertyName.equals(name1));
        assertFalse(names.hasMoreElements());
    }

    @Test
    public void testClearPropertiesEmptiesProperties() throws Exception
    {
        String propertyName = "myProperty";

        _testMessage.setObjectProperty(propertyName, "string");

        _testMessage.clearProperties();

        //check the prop no longer exists, returns null
        assertFalse(_testMessage.propertyExists(propertyName));
        assertNull(_testMessage.getObjectProperty(propertyName));

        //check there are no properties
        Enumeration<?> names = _testMessage.getPropertyNames();
        int numProps = 0;
        while(names.hasMoreElements())
        {
            numProps++;
        }

        assertEquals(0, numProps);
    }

    // ======= String Properties =========

    @Test
    public void testSetGetStringProperty() throws Exception
    {
        //null property value
        String propertyName = "myNullProperty";
        String propertyValue = null;

        assertFalse(_testMessage.propertyExists(propertyName));
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertTrue(_testMessage.propertyExists(propertyName));
        assertEquals(propertyValue, _testMessage.getStringProperty(propertyName));

        //non-null property value
        propertyName = "myProperty";
        propertyValue = "myPropertyValue";

        assertFalse(_testMessage.propertyExists(propertyName));
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertTrue(_testMessage.propertyExists(propertyName));
        assertEquals(propertyValue, _testMessage.getStringProperty(propertyName));
    }

    @Test
    public void testSetStringGetLegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        String propertyValue;

        //boolean
        propertyValue =  "true";
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertGetPropertyEquals(_testMessage, propertyName, Boolean.valueOf(propertyValue), Boolean.class);

        //byte
        propertyValue =  String.valueOf(Byte.MAX_VALUE);
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertGetPropertyEquals(_testMessage, propertyName, Byte.valueOf(propertyValue), Byte.class);

        //short
        propertyValue =  String.valueOf(Short.MAX_VALUE);
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertGetPropertyEquals(_testMessage, propertyName, Short.valueOf(propertyValue), Short.class);

        //int
        propertyValue =  String.valueOf(Integer.MAX_VALUE);
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertGetPropertyEquals(_testMessage, propertyName, Integer.valueOf(propertyValue), Integer.class);

        //long
        propertyValue =  String.valueOf(Long.MAX_VALUE);
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertGetPropertyEquals(_testMessage, propertyName, Long.valueOf(propertyValue), Long.class);

        //float
        propertyValue =  String.valueOf(Float.MAX_VALUE);
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertGetPropertyEquals(_testMessage, propertyName, Float.valueOf(propertyValue), Float.class);

        //double
        propertyValue =  String.valueOf(Double.MAX_VALUE);
        _testMessage.setStringProperty(propertyName, propertyValue);
        assertGetPropertyEquals(_testMessage, propertyName, Double.valueOf(propertyValue), Double.class);
    }

    // ======= boolean Properties =========

    @Test
    public void testSetBooleanGetLegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        boolean propertyValue = true;

        _testMessage.setBooleanProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getBooleanProperty(propertyName));

        assertGetPropertyEquals(_testMessage, propertyName, String.valueOf(propertyValue), String.class);
    }

    @Test
    public void testSetBooleanGetIllegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        boolean propertyValue = true;

        _testMessage.setBooleanProperty(propertyName, propertyValue);

        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Byte.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Short.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Integer.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Long.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Float.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Double.class);
    }

    // ======= byte Properties =========

    @Test
    public void testSetByteGetLegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        byte propertyValue = (byte)1;

        _testMessage.setByteProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getByteProperty(propertyName));

        assertGetPropertyEquals(_testMessage, propertyName, String.valueOf(propertyValue), String.class);
        assertGetPropertyEquals(_testMessage, propertyName, Short.valueOf(propertyValue), Short.class);
        assertGetPropertyEquals(_testMessage, propertyName, Integer.valueOf(propertyValue), Integer.class);
        assertGetPropertyEquals(_testMessage, propertyName, Long.valueOf(propertyValue), Long.class);
    }

    @Test
    public void testSetByteGetIllegalPropertyThrowsMFE() throws Exception
    {
        String propertyName = "myProperty";
        byte propertyValue = (byte)1;

        _testMessage.setByteProperty(propertyName, propertyValue);

        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Boolean.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Float.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Double.class);
    }

    // ======= short Properties =========

    @Test
    public void testSetShortGetLegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        short propertyValue = (short)1;

        _testMessage.setShortProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getShortProperty(propertyName));

        assertGetPropertyEquals(_testMessage, propertyName, String.valueOf(propertyValue), String.class);
        assertGetPropertyEquals(_testMessage, propertyName, Integer.valueOf(propertyValue), Integer.class);
        assertGetPropertyEquals(_testMessage, propertyName, Long.valueOf(propertyValue), Long.class);
    }

    @Test
    public void testSetShortGetIllegalPropertyThrowsMFE() throws Exception
    {
        String propertyName = "myProperty";
        short propertyValue = (short)1;

        _testMessage.setShortProperty(propertyName, propertyValue);

        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Boolean.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Byte.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Float.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Double.class);
    }

    // ======= int Properties =========

    @Test
    public void testSetIntGetLegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        int propertyValue = (int)1;

        _testMessage.setIntProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getIntProperty(propertyName));

        assertGetPropertyEquals(_testMessage, propertyName, String.valueOf(propertyValue), String.class);
        assertGetPropertyEquals(_testMessage, propertyName, Long.valueOf(propertyValue), Long.class);
    }

    @Test
    public void testSetIntGetIllegalPropertyThrowsMFE() throws Exception
    {
        String propertyName = "myProperty";
        int propertyValue = (int)1;

        _testMessage.setIntProperty(propertyName, propertyValue);

        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Boolean.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Byte.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Short.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Float.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Double.class);
    }

    // ======= long Properties =========

    @Test
    public void testSetLongGetLegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        long propertyValue = Long.MAX_VALUE;

        _testMessage.setLongProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getLongProperty(propertyName));

        assertGetPropertyEquals(_testMessage, propertyName, String.valueOf(propertyValue), String.class);
    }

    @Test
    public void testSetLongGetIllegalPropertyThrowsMFE() throws Exception
    {
        String propertyName = "myProperty";
        long propertyValue = Long.MAX_VALUE;

        _testMessage.setLongProperty(propertyName, propertyValue);

        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Boolean.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Byte.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Short.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Integer.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Float.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Double.class);
    }

    // ======= float Properties =========

    @Test
    public void testSetFloatGetLegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        float propertyValue = Float.MAX_VALUE;

        _testMessage.setFloatProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getFloatProperty(propertyName), 0.0);

        assertGetPropertyEquals(_testMessage, propertyName, String.valueOf(propertyValue), String.class);
        assertGetPropertyEquals(_testMessage, propertyName, Double.valueOf(propertyValue), Double.class);
    }

    @Test
    public void testSetFloatGetIllegalPropertyThrowsMFE() throws Exception
    {
        String propertyName = "myProperty";
        float propertyValue = Float.MAX_VALUE;

        _testMessage.setFloatProperty(propertyName, propertyValue);

        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Boolean.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Byte.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Short.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Integer.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Long.class);
    }

    // ======= double Properties =========

    @Test
    public void testSetDoubleGetLegalProperty() throws Exception
    {
        String propertyName = "myProperty";
        double propertyValue = Double.MAX_VALUE;

        _testMessage.setDoubleProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getDoubleProperty(propertyName), 0.0);

        assertGetPropertyEquals(_testMessage, propertyName, String.valueOf(propertyValue), String.class);
    }

    @Test
    public void testSetDoubleGetIllegalPropertyThrowsMFE() throws Exception
    {
        String propertyName = "myProperty";
        double propertyValue = Double.MAX_VALUE;

        _testMessage.setDoubleProperty(propertyName, propertyValue);

        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Boolean.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Byte.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Short.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Integer.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Long.class);
        assertGetPropertyThrowsMessageFormatException(_testMessage, propertyName, Float.class);
    }

    // ====== JMSDestination =======

    @Test
    public void testGetJMSDestinationOnNewMessage() throws Exception
    {
        //Should be null as it has not been set explicitly, and
        // the message has not been sent anywhere
        assertNull(_testMessage.getJMSDestination());
    }

    @Test
    public void testSetJMSDestinationOnNewMessageUsingQueue() throws Exception
    {
        assertNull(_testAmqpMessage.getTo());

        _testMessage.setJMSDestination(_mockQueue);

        assertNotNull(_testAmqpMessage.getTo());
        assertEquals(_mockQueueName, _testAmqpMessage.getTo());

        assertTrue(_testAmqpMessage.messageAnnotationExists(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
        assertEquals(DestinationHelper.QUEUE_ATTRIBUTES_STRING,
                     _testAmqpMessage.getMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
    }

    @Test
    public void testSetJMSDestinationOnNewMessageUsingTopic() throws Exception
    {
        assertNull(_testAmqpMessage.getTo());

        _testMessage.setJMSDestination(_mockTopic);

        assertNotNull(_testAmqpMessage.getTo());
        assertEquals(_mockTopicName, _testAmqpMessage.getTo());

        assertTrue(_testAmqpMessage.messageAnnotationExists(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
        assertEquals(DestinationHelper.TOPIC_ATTRIBUTES_STRING,
                     _testAmqpMessage.getMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
    }

    @Test
    public void testSetJMSDestinationNullOnReceivedMessageWithToAndTypeAnnotationClearsTheAnnotation() throws Exception
    {
        _testAmqpMessage.setTo(_mockTopicName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.TOPIC_ATTRIBUTES_STRING);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertNotNull("expected JMSDestination value not present", _testMessage.getJMSDestination());
        assertTrue(_testAmqpMessage.messageAnnotationExists(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));

        _testMessage.setJMSDestination(null);

        assertNull("expected JMSDestination value to be null", _testMessage.getJMSDestination());
        assertFalse(_testAmqpMessage.messageAnnotationExists(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
    }

    @Test
    public void testSetGetJMSDestinationOnNewMessage() throws Exception
    {
        _testMessage.setJMSDestination(_mockQueue);
        assertNotNull(_testMessage.getJMSDestination());
        assertSame(_mockQueue, _testMessage.getJMSDestination());
    }

    @Test
    public void testGetJMSDestinationOnReceivedMessageWithToButWithoutToTypeAnnotation() throws Exception
    {
        _testAmqpMessage.setTo(_mockQueueName);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertNotNull("expected JMSDestination value not present", _testMessage.getJMSDestination());

        Destination newDestinationExpected = new DestinationImpl(_mockQueueName);
        assertEquals(newDestinationExpected, _testMessage.getJMSDestination());
    }

    @Test
    public void testGetJMSDestinationOnReceivedMessageWithToAndTypeAnnotationForTopic() throws Exception
    {
        _testAmqpMessage.setTo(_mockTopicName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.TOPIC_ATTRIBUTES_STRING);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertNotNull("expected JMSDestination value not present", _testMessage.getJMSDestination());

        Topic newDestinationExpected = new DestinationHelper().createTopic(_mockTopicName);
        assertEquals(newDestinationExpected, _testMessage.getJMSDestination());
    }

    @Test
    public void testGetJMSDestinationOnReceivedMessageWithToAndTypeAnnotationForQueue() throws Exception
    {
        _testAmqpMessage.setTo(_mockQueueName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.QUEUE_ATTRIBUTES_STRING);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertNotNull("expected JMSDestination value not present", _testMessage.getJMSDestination());

        Queue newDestinationExpected = new DestinationHelper().createQueue(_mockQueueName);
        assertEquals(newDestinationExpected, _testMessage.getJMSDestination());
    }

    // ====== JMSReplyTo =======

    @Test
    public void testGetJMSReplyToOnNewMessage() throws Exception
    {
        //Should be null as it has not been set explicitly, and
        // the message has not been received from anywhere
        assertNull(_testMessage.getJMSReplyTo());
    }

    @Test
    public void testSetJMSJMSReplyToOnNewMessageUsingQueue() throws Exception
    {
        assertNull(_testAmqpMessage.getReplyTo());

        _testMessage.setJMSReplyTo(_mockQueue);

        assertNotNull(_testAmqpMessage.getReplyTo());
        assertEquals(_mockQueueName, _testAmqpMessage.getReplyTo());

        assertTrue(_testAmqpMessage.messageAnnotationExists(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
        assertEquals(DestinationHelper.QUEUE_ATTRIBUTES_STRING,
                     _testAmqpMessage.getMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
    }

    @Test
    public void testSetJMSReplyToOnNewMessageUsingTopic() throws Exception
    {
        assertNull(_testAmqpMessage.getReplyTo());

        _testMessage.setJMSReplyTo(_mockTopic);

        assertNotNull(_testAmqpMessage.getReplyTo());
        assertEquals(_mockTopicName, _testAmqpMessage.getReplyTo());

        assertTrue(_testAmqpMessage.messageAnnotationExists(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
        assertEquals(DestinationHelper.TOPIC_ATTRIBUTES_STRING,
                     _testAmqpMessage.getMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
    }

    @Test
    public void testSetJMSReplyToNullOnReceivedMessageWithReplyToAndTypeAnnotationClearsTheAnnotation() throws Exception
    {
        _testAmqpMessage.setReplyTo(_mockTopicName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.TOPIC_ATTRIBUTES_STRING);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertNotNull("expected JMSReplyTo value not present", _testMessage.getJMSReplyTo());
        assertTrue(_testAmqpMessage.messageAnnotationExists(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));

        _testMessage.setJMSReplyTo(null);

        assertNull("expected JMSReplyTo value to be null", _testMessage.getJMSReplyTo());
        assertFalse(_testAmqpMessage.messageAnnotationExists(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
    }

    @Test
    public void testSetGetJMSReplyToNewMessage() throws Exception
    {
        _testMessage.setJMSReplyTo(_mockQueue);
        assertNotNull(_testMessage.getJMSReplyTo());
        assertSame(_mockQueue, _testMessage.getJMSReplyTo());
    }

    @Test
    public void testGetJMSReplyToReceivedMessageWithReplyToButWithoutReplyToTypeAnnotation() throws Exception
    {
        _testAmqpMessage.setReplyTo(_mockQueueName);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertNotNull("expected JMSReplyTo value not present", _testMessage.getJMSReplyTo());

        Destination newDestinationExpected = new DestinationImpl(_mockQueueName);
        assertEquals(newDestinationExpected, _testMessage.getJMSReplyTo());
    }

    @Test
    public void testGetJMSReplyToOnReceivedMessageWithReplyToAndTypeAnnotationForTopic() throws Exception
    {
        _testAmqpMessage.setReplyTo(_mockTopicName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.TOPIC_ATTRIBUTES_STRING);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertNotNull("expected JMSReplyTo value not present", _testMessage.getJMSReplyTo());

        Topic newDestinationExpected = new DestinationHelper().createTopic(_mockTopicName);
        assertEquals(newDestinationExpected, _testMessage.getJMSReplyTo());
    }

    @Test
    public void testGetJMSReplyToReceivedMessageWithReplyToAndTypeAnnotationForQueue() throws Exception
    {
        _testAmqpMessage.setReplyTo(_mockQueueName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.QUEUE_ATTRIBUTES_STRING);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertNotNull("expected JMSReplyTo value not present", _testMessage.getJMSReplyTo());

        Queue newDestinationExpected = new DestinationHelper().createQueue(_mockQueueName);
        assertEquals(newDestinationExpected, _testMessage.getJMSReplyTo());
    }

    // ====== JMSTimestamp =======

    @Test
    public void testGetJMSTimestampOnNewMessage() throws Exception
    {
        assertEquals("expected JMSTimestamp value not present", 0, _testMessage.getJMSTimestamp());
    }

    @Test
    public void testSetGetJMSTimestampOnNewMessage() throws Exception
    {
        long timestamp = System.currentTimeMillis();

        _testMessage.setJMSTimestamp(timestamp);
        assertEquals("expected JMSTimestamp value not present", timestamp, _testMessage.getJMSTimestamp());
    }

    @Test
    public void testSetJMSTimestampOnNewMessage() throws Exception
    {
        assertNull(_testAmqpMessage.getCreationTime());

        long timestamp = System.currentTimeMillis();
        _testMessage.setJMSTimestamp(timestamp);

        assertNotNull(_testAmqpMessage.getCreationTime());
        assertEquals(timestamp, _testAmqpMessage.getCreationTime().longValue());
    }

    @Test
    public void testGetJMSTimestampOnReceivedMessageWithCreationTime() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        _testAmqpMessage.setCreationTime(timestamp);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertEquals("expected JMSTimestamp value not present", timestamp, _testMessage.getJMSTimestamp());
    }

    @Test
    public void testSetJMSTimestampToZeroOnReceivedMessageWithCreationTimeSetsUnderlyingCreationTimeNull() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        _testAmqpMessage.setCreationTime(timestamp);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        _testMessage.setJMSTimestamp(0);

        assertEquals("expected JMSExpiration value not present", 0L, _testMessage.getJMSExpiration());
        assertNull(_testAmqpMessage.getCreationTime());
    }

    // ====== JMSExpiration =======

    @Test
    public void testGetJMSExpirationOnNewMessage() throws Exception
    {
        assertEquals("expected JMSExpiration value not present", 0, _testMessage.getJMSExpiration());
    }

    @Test
    public void testSetGetJMSExpirationOnNewMessage() throws Exception
    {
        long timestamp = System.currentTimeMillis();

        _testMessage.setJMSExpiration(timestamp);
        assertEquals("expected JMSExpiration value not present", timestamp, _testMessage.getJMSExpiration());
    }

    @Test
    public void testSetJMSExpirationOnNewMessage() throws Exception
    {
        assertNull(_testAmqpMessage.getAbsoluteExpiryTime());

        Long timestamp = System.currentTimeMillis();
        _testMessage.setJMSExpiration(timestamp);

        assertEquals(timestamp, _testAmqpMessage.getAbsoluteExpiryTime());
    }

    @Test
    public void testGetJMSExpirationOnReceivedMessageWithAbsoluteExpiryTimeAndTtl() throws Exception
    {
        long creationTime = 123456789;
        long ttl = 789L;
        long expiration = creationTime + ttl;
        _testAmqpMessage.setTtl(ttl);
        _testAmqpMessage.setAbsoluteExpiryTime(expiration);

        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertEquals("expected JMSExpiration value not present", expiration, _testMessage.getJMSExpiration());
    }

    @Test
    public void testGetJMSExpirationOnReceivedMessageWithAbsoluteExpiryTimeButNoTtl() throws Exception
    {
        long expiration = System.currentTimeMillis();
        _testAmqpMessage.setAbsoluteExpiryTime(expiration);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertEquals("expected JMSExpiration value not present", expiration, _testMessage.getJMSExpiration());
    }

    @Test
    public void testGetJMSExpirationOnReceivedMessageWithTtlButNoAbsoluteExpiry() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        long ttl = 999999L;
        _testAmqpMessage.setTtl(ttl);

        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        long jmsExpiration = _testMessage.getJMSExpiration();

        //as there is no absolute-expiry-time, JMSExpiration will be based on 'current time' + ttl, so check equality within a given delta
        assertEquals("expected JMSExpiration value not present", timestamp + ttl, jmsExpiration, 3000);

        Thread.sleep(1);

        //check we get the same value again
        assertEquals("different JMSExpiration on subsequent calls", jmsExpiration, _testMessage.getJMSExpiration());
    }

    @Test
    public void testSetJMSExpirationToZeroOnReceivedMessageWithAbsoluteExpiryTimeSetsUnderlyingExpiryNull() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        _testAmqpMessage.setAbsoluteExpiryTime(timestamp);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        _testMessage.setJMSExpiration(0);

        assertEquals("expected JMSExpiration value not present", 0L, _testMessage.getJMSExpiration());
        assertNull(_testAmqpMessage.getAbsoluteExpiryTime());
    }

    /**
     * As we are basing getJMSExpiration for incoming messages on the ttl field if absolute-expiry-time is missing, and must not set
     * absolute-expiry-time if JMSExpiration is 0, ensure that setting JMSExpiration to 0 results in getJMSExpiration
     * returning 0 if an incoming message had only the ttl field set.
     */
    @Test
    public void testSetJMSExpirationToZeroOnReceivedMessageWithTtlFieldsResultsInGetJMSExpirationReturningZero() throws Exception
    {
        long ttl = 789L;
        _testAmqpMessage.setTtl(ttl);

        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        _testMessage.setJMSExpiration(0);

        assertEquals("expected JMSExpiration value not present", 0L, _testMessage.getJMSExpiration());
    }

    /**
     * As we are basing getJMSExpiration either on the absolute-expiry-time or calculating it based on the ttl field,
     * ensure that setting JMSExpiration to 0 results in getJMSExpiration returning 0 if an incoming message had both
     * absolute-expiry-time and ttl fields set.
     */
    @Test
    public void testSetJMSExpirationToZeroOnReceivedMessageWithAbsoluteExpiryAndTtlFieldsResultsInGetJMSExpirationReturningZero() throws Exception
    {
        long ttl = 789L;
        long timestamp = System.currentTimeMillis();
        _testAmqpMessage.setTtl(ttl);
        _testAmqpMessage.setAbsoluteExpiryTime(timestamp);

        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        _testMessage.setJMSExpiration(0);

        assertEquals("expected JMSExpiration value not present", 0L, _testMessage.getJMSExpiration());
    }

    // ====== JMSPriority =======

    @Test
    public void testGetJMSPriorityOnNewMessage() throws Exception
    {
        assertEquals("expected JMSPriority value not present", Message.DEFAULT_PRIORITY, _testMessage.getJMSPriority());
    }

    @Test
    public void testSetGetJMSPriorityOnNewMessage() throws Exception
    {
        int priority = Message.DEFAULT_PRIORITY + 1;
        _testMessage.setJMSPriority(priority);

        //check value was set on underlying message
        assertEquals(priority, _testAmqpMessage.getPriority());

        //check retrieving value yeilds expected value
        assertEquals("expected JMSPriority value not present", priority, _testMessage.getJMSPriority());
    }

    /**
     * AMQP supports more priority levels (256, 0-255) than JMS does (10, 0-9). If a message is received
     * with a priority of over 9, JMSPriority should be capped at 9.
     * @throws Exception
     */
    @Test
    public void testGetJMSPriorityOnReceivedMessageWithNonJmsPriorityIsCappedAt9() throws Exception
    {
        short priority = 200;
        _testAmqpMessage.setPriority(priority);

        //create a received message
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertEquals("expected JMSPriority value to be capped at 9", 9, _testMessage.getJMSPriority());
    }

    // ====== JMSMessageID =======

    @Test
    public void testGetJMSMessageIDOnNewMessage() throws Exception
    {
        //Should be null as it has not been set explicitly, and
        //the message has not been sent anywhere
        assertNull(_testMessage.getJMSMessageID());
    }

    /**
     * Test that {@link MessageImpl#setJMSMessageID(String)} accepts null and clears an existing value
     */
    @Test
    public void testSetJMSMessageIDAcceptsNull() throws Exception
    {
        //test setting null on fresh message is accepted
        _testMessage.setJMSMessageID(null);
        assertNull("JMSMessageID should still be null", _testMessage.getJMSMessageID());

        //test that setting null clears an existing value
        _testMessage.setJMSMessageID("ID:something");
        assertNotNull("JMSMessageID should not be null anymore", _testMessage.getJMSMessageID());
        assertNotNull("Underlying message id should not be null anymore", _testAmqpMessage.getMessageId());

        _testMessage.setJMSMessageID(null);
        assertNull("JMSMessageID should be null again", _testMessage.getJMSMessageID());
        assertNull("Underlying message id should be null again", _testAmqpMessage.getMessageId());
    }

    /**
     * Test that {@link MessageImpl#setJMSMessageID(String)} throws JMSException when
     * the provided non-null value doesn't begin with "ID:".
     */
    @Test
    public void testSetJMSMessageIDOnNewMessageThrowsExceptionForNonIdValues() throws Exception
    {
        try
        {
            //"ID:" is present, but in the middle instead of required prefix
            _testMessage.setJMSMessageID("Central:ID:Included");
            fail("Expected exception when setting non-null value without the required JMSMessageID 'ID:' prefix");
        }
        catch(JMSException jmse)
        {
            //expected
        }

        try
        {
            //"ID:" isn't present at all.
            _testMessage.setJMSMessageID("MissingEntirely");
            fail("Expected exception when setting non-null value without the required JMSMessageID 'ID:' prefix");
        }
        catch(JMSException jmse)
        {
            //expected
        }

        try
        {
            //Prefix exists, but with the wrong case (it must be "ID:").
            _testMessage.setJMSMessageID("id:WrongCase");
            fail("Expected exception when setting non-null value without the required JMSMessageID 'ID:' prefix");
        }
        catch(JMSException jmse)
        {
            //expected
        }
    }

    /**
     * Test that {@link MessageImpl#setJMSMessageID(String)} does not throw a JMSException when
     * the provided value is a message id value indicating an encoded AMQP type, but which couldnt
     * be converted to that type due to malformed content. This is necessary because we may
     * have a JMSMessageID value set on us by a foreign provider while sending our message objects.
     */
    @Test
    public void testSetJMSMessageIDDoesNotThrowJMSExceptionWhenProvidedMalformedIDEncoding() throws Exception
    {
        //craft a message id format value which contains content thatnot convertible to
        //the indicated AMQP type encoding: "hello" is not a ulong.
        String baseId = MessageIdHelper.AMQP_ULONG_PREFIX + "hello";
        String jmsId = MessageIdHelper.JMS_ID_PREFIX + baseId;

        try
        {
            _testMessage.setJMSMessageID(jmsId);
            assertEquals("underlying message did not have expected value set", baseId, _testAmqpMessage.getMessageId());
        }
        catch(JMSException jmse)
        {
            fail("No exception should have been thrown, but got: " + jmse);
        }
    }

    /**
     * Test that {@link MessageImpl#setJMSMessageID(String)} sets the expected value
     * on the underlying message, i.e the JMS message ID minus the "ID:" prefix.
     */
    @Test
    public void testSetJMSMessageIDSetsUnderlyingMessageWithString() throws Exception
    {
        String baseId = "something";
        String jmsId = "ID:" + baseId;

        _testMessage.setJMSMessageID(jmsId);

        assertNotNull("Underlying message id should not be null", _testAmqpMessage.getMessageId());
        assertEquals("Underlying message id value was not as expected", baseId, _testAmqpMessage.getMessageId());
    }

    /**
     * Test that receiving a message with a string typed message-id value results in the
     * expected JMSMessageID value being returned, i.e. the base string plus the JMS "ID:" prefix.
     */
    @Test
    public void testGetJMSMessageIdOnReceivedMessageWithString() throws Exception
    {
        getJMSMessageIdOnReceivedMessageWithStringTestImpl(false);
    }

    /**
     * Test that receiving a message with a string typed message-id value results in the
     * expected JMSMessageID value being returned, i.e. the base string plus the JMS "ID:" prefix.
     */
    @Test
    public void testGetJMSMessageIdOnReceivedMessageWithStringAlreadyContainingPrefix() throws Exception
    {
        getJMSMessageIdOnReceivedMessageWithStringTestImpl(true);
    }

    private void getJMSMessageIdOnReceivedMessageWithStringTestImpl(boolean prefixAlreadyExists) throws JMSException
    {
        String baseId = "something";
        if(prefixAlreadyExists)
        {
            baseId = "ID:" + baseId;
        }

        String expectedJmsMessageId = "ID:" + baseId;

        _testAmqpMessage.setMessageId(baseId);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertEquals("expected JMSMessageID value not present", expectedJmsMessageId, _testMessage.getJMSMessageID());
    }

    // ====== JMSCorrelationID =======

    @Test
    public void testGetJMSCorrelationIDIsNullOnNewMessage() throws Exception
    {
        assertNull("JMSCorrelationID should be null on new message", _testMessage.getJMSCorrelationID());
    }

    /**
     * Test that the message annotation used to denote an application-specific JMSCorrelationID
     * value does not exist on new messages (which have no correlation-id, as per
     * {@link #testGetJMSCorrelationIDIsNullOnNewMessage})
     */
    @Test
    public void testAppSpecificCorrelationIdAnnotationDoesNotExistOnNewMessage() throws Exception
    {
        assertFalse("MessageAnnotation should not exist to indicate app-specific correlation-id, since there is no correlation-id",
                _testAmqpMessage.messageAnnotationExists(ClientProperties.X_OPT_APP_CORRELATION_ID));
    }

    /**
     * Test that {@link MessageImpl#setJMSCorrelationID(String)} accepts null and clears any
     * existing value that happened to be a message id.
     */
    @Test
    public void testSetJMSCorrelationIDAcceptsNullAndClearsPreviousMessageIdValue() throws Exception
    {
        //test setting null on fresh message is accepted
        _testMessage.setJMSCorrelationID(null);
        assertNull("JMSCorrelationID should still be null", _testMessage.getJMSCorrelationID());

        //test that setting null clears an existing value
        _testMessage.setJMSCorrelationID("ID:something");
        assertNotNull("JMSCorrelationID should not be null anymore", _testMessage.getJMSCorrelationID());
        assertNotNull("Underlying correlation id should not be null anymore", _testAmqpMessage.getCorrelationId());

        _testMessage.setJMSCorrelationID(null);
        assertNull("JMSCorrelationID should be null again", _testMessage.getJMSCorrelationID());
        assertNull("Underlying correlation id should be null again", _testAmqpMessage.getCorrelationId());
    }

    /**
     * Test that {@link MessageImpl#setJMSCorrelationID(String)} accepts null and clears an existing app-specific
     * value, additionally clearing the message annotation indicating the value was app-specific.
     */
    @Test
    public void testSetJMSCorrelationIDAcceptsNullAndClearsPreviousAppSpecificValue() throws Exception
    {
        //test that setting null clears an existing value
        _testMessage.setJMSCorrelationID("app-specific");
        assertNotNull("JMSCorrelationID should not be null anymore", _testMessage.getJMSCorrelationID());
        assertNotNull("Underlying correlation id should not be null anymore", _testAmqpMessage.getCorrelationId());

        _testMessage.setJMSCorrelationID(null);
        assertNull("JMSCorrelationID should be null again", _testMessage.getJMSCorrelationID());
        assertNull("Underlying correlation id should be null again", _testAmqpMessage.getCorrelationId());

        assertFalse("MessageAnnotation should not exist to indicate app-specific correlation-id, since there is no correlation-id",
                _testAmqpMessage.messageAnnotationExists(ClientProperties.X_OPT_APP_CORRELATION_ID));
    }

    /**
     * Test that {@link MessageImpl#setJMSCorrelationID(String)} throws a JMSException when
     * the provided value is a message id value indicating an encoded AMQP type, but which cant
     * be converted to that type.
     */
    @Test
    public void testSetJMSCorrelationIDThrowsJMSExceptionWhenProvidedIllegalIDEncoding() throws Exception
    {
        //craft a message id format value which contains content thatnot convertible to
        //the indicated AMQP type encoding: "hello" is not a ulong.
        String jmsId = MessageIdHelper.JMS_ID_PREFIX + MessageIdHelper.AMQP_ULONG_PREFIX + "hello";

        try
        {
            _testMessage.setJMSCorrelationID(jmsId);
            fail("expected exception to be thrown");
        }
        catch(IdConversionException ice)
        {
            assertTrue("Exception exception to be a JMSException", ice instanceof JMSException);
        }
    }

    /**
     * Test that {@link MessageImpl#setJMSCorrelationID(String)} sets the expected value
     * on the underlying message, i.e the JMS CorrelationID minus the "ID:" prefix
     */
    @Test
    public void testSetJMSCorrelationIDSetsUnderlyingMessageWithMessageIdString() throws Exception
    {
        String baseId = "something";
        String jmsId = "ID:" + baseId;

        _testMessage.setJMSCorrelationID(jmsId);

        assertNotNull("Underlying correlation id should not be null", _testAmqpMessage.getCorrelationId());
        assertEquals("Underlying correlation id value was not as expected", baseId, _testAmqpMessage.getCorrelationId());
    }

    /**
     * Test that {@link MessageImpl#setJMSCorrelationID(String)} with a value that happens to be
     * a message id does not set the annotation on the underlying message
     * to indicate the correlation-id value is application-specific (sicne it isnt).
     */
    @Test
    public void testSetJMSCorrelationIDDoesntSetsUnderlyingMessageAnnotationWithMessageIdString() throws Exception
    {
        String baseId = "something";
        String jmsId = "ID:" + baseId;

        _testMessage.setJMSCorrelationID(jmsId);

        assertFalse("MessageAnnotation should not exist to indicate app-specific correlation-id",
                        _testAmqpMessage.messageAnnotationExists(ClientProperties.X_OPT_APP_CORRELATION_ID));
    }

    /**
     * Test that {@link MessageImpl#setJMSCorrelationID(String)} sets the expected value
     * on the underlying message when provided an application-specific string.
     */
    @Test
    public void testSetJMSCorrelationIDSetsUnderlyingMessageWithAppSpecificString() throws Exception
    {
        String baseId = "app-specific";

        _testMessage.setJMSCorrelationID(baseId);

        assertNotNull("Underlying correlation id should not be null", _testAmqpMessage.getCorrelationId());
        assertEquals("Underlying correlation id value was not as expected", baseId, _testAmqpMessage.getCorrelationId());
    }

    /**
     * Test that {@link MessageImpl#setJMSCorrelationID(String)} sets the expected
     * message annotation on the underlying message when provided an application-specific string
     */
    @Test
    public void testSetJMSCorrelationIDSetsUnderlyingMessageAnnotationWithAppSpecificString() throws Exception
    {
        String baseId = "app-specific";

        _testMessage.setJMSCorrelationID(baseId);

        assertTrue("MessageAnnotation should exist to indicate app-specific correlation-id",
                        _testAmqpMessage.messageAnnotationExists(ClientProperties.X_OPT_APP_CORRELATION_ID));
        assertEquals("MessageAnnotation should be true to indicate app-specific correlation-id", Boolean.TRUE,
                        _testAmqpMessage.getMessageAnnotation(ClientProperties.X_OPT_APP_CORRELATION_ID));
    }

    /**
     * Test that {@link MessageImpl#getJMSCorrelationID()} returns the expected value for an
     * application-specific string when the {@link ClientProperties#X_OPT_APP_CORRELATION_ID}
     * message annotation is set true.
     */
    @Test
    public void testGetJMSCorrelationIDWithReceivedMessageWithAppSpecificString() throws Exception
    {
        String appSpecific = "app-specific";
        _testAmqpMessage.setMessageAnnotation(ClientProperties.X_OPT_APP_CORRELATION_ID, true);
        _testAmqpMessage.setCorrelationId(appSpecific);

        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        String jmsCorrelationID = _testMessage.getJMSCorrelationID();
        assertNotNull("expected JMSCorrelationID to be app-specific string", jmsCorrelationID);
        assertFalse("expected JMSCorrelationID to be app-specific string, should not contain ID: prefix", jmsCorrelationID.startsWith(MessageIdHelper.JMS_ID_PREFIX));
        assertEquals("expected JMSCorrelationID to be app-specific string", appSpecific, jmsCorrelationID);
    }

    /**
     * Test that receiving a message with a string typed correlation-id value results in the
     * expected JMSCorrelationID value being returned, i.e. the base string plus the JMS "ID:" prefix.
     */
    @Test
    public void testGetJMSCorrelationIDOnReceivedMessageWithString() throws Exception
    {
        getJMSCorrelationIDOnReceivedMessageWithStringTestImpl(false);
    }

    /**
     * Test that receiving a message with a string typed correlation-id value results in the
     * expected JMSCorrelationID value being returned, i.e. the base string plus the JMS "ID:" prefix.
     */
    @Test
    public void testGetJMSCorrelationIDOnReceivedMessageWithStringAlreadyContainingPrefix() throws Exception
    {
        getJMSCorrelationIDOnReceivedMessageWithStringTestImpl(true);
    }

    private void getJMSCorrelationIDOnReceivedMessageWithStringTestImpl(boolean prefixAlreadyExists) throws JMSException
    {
        String baseId = "something";
        if(prefixAlreadyExists)
        {
            baseId = "ID:" + baseId;
        }

        String expectedJmsCorrelationId = "ID:" + baseId;

        _testAmqpMessage.setCorrelationId(baseId);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertEquals("expected JMSCorrelationID value not present", expectedJmsCorrelationId, _testMessage.getJMSCorrelationID());
    }

    @Test
    public void testGetJMSCorrelationIDAsBytesOnNewMessageReturnsNull() throws Exception
    {
        assertNull("expected JMSCorrelationID bytes to be null on new message", _testMessage.getJMSCorrelationIDAsBytes());
    }

    /**
     * Test that receiving a message with a binary typed correlation-id value results in the expected
     * JMSCorrelationID value being returned via {@link MessageImpl#getJMSCorrelationIDAsBytes()}
     * i.e. the bytes contained in the binary value.
     */
    @Test
    public void testGetJMSCorrelationIDAsBytesOnReceivedMessageWithBinaryId() throws Exception
    {
        String baseId = "myBaseId";
        byte[] expectedJmsCorrelationIdAsBytes = baseId.getBytes();

        ByteBuffer binary = ByteBuffer.wrap(expectedJmsCorrelationIdAsBytes);
        _testAmqpMessage.setCorrelationId(binary);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertTrue("expected JMSCorrelationID bytes value not present", Arrays.equals(expectedJmsCorrelationIdAsBytes, _testMessage.getJMSCorrelationIDAsBytes()));
    }

    /**
     * Test that the JMSCorrelationID value being returned via {@link MessageImpl#getJMSCorrelationIDAsBytes()}
     * is a copy of the underlying data, i.e. not the value provided, and different on each call.
     */
    @Test
    public void testGetJMSCorrelationIDAsBytesOnReceivedMessageWithBinaryIdReturnsArrayCopy() throws Exception
    {
        String baseId = "myBaseId";
        byte[] expectedJmsCorrelationIdAsBytes = baseId.getBytes();

        ByteBuffer binary = ByteBuffer.wrap(expectedJmsCorrelationIdAsBytes);
        _testAmqpMessage.setCorrelationId(binary);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        //check the return value doesn't contain the original bytes
        byte[] jmsCorrelationIDAsBytes1 = _testMessage.getJMSCorrelationIDAsBytes();
        assertNotSame("expected byte arrays to be different objects", expectedJmsCorrelationIdAsBytes, jmsCorrelationIDAsBytes1);

        //check a second call returns a further different array object
        assertNotSame("expected byte arrays to be different objects", jmsCorrelationIDAsBytes1, _testMessage.getJMSCorrelationIDAsBytes());
    }

    /**
     * Test that receiving a message with a binary typed correlation-id value results in the expected
     * JMSCorrelationID value being returned via {@link MessageImpl#getJMSCorrelationIDAsBytes()}
     * i.e. the bytes contained in the binary value.
     */
    @Test
    public void testGetJMSCorrelationIDAsBytesOnReceivedMessageWithStringIdThrowsJMSE() throws Exception
    {
        String baseId = "myBaseId";

        _testAmqpMessage.setCorrelationId(baseId);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        try
        {
            _testMessage.getJMSCorrelationIDAsBytes();
            fail("expected exception now thrown");
        }
        catch(JMSException jmse)
        {
            //expected
        }
    }

    /**
     * Test that the setting a JMSCorrelationID value via {@link MessageImpl#getJMSCorrelationIDAsBytes()}
     * results in a copy of the bytes being set on the underlying message, i.e. not the value provided.
     */
    @Test
    public void testSetJMSCorrelationIDAsBytesOnNewMessageWithBinaryIdSetsArrayCopyOnUnderlying() throws Exception
    {
        String baseId = "myBaseId";
        byte[] providedJmsCorrelationIdAsBytes = baseId.getBytes();

        assertNull("unexpected underlying correlationId value found", _testAmqpMessage.getCorrelationId());
        _testMessage.setJMSCorrelationIDAsBytes(providedJmsCorrelationIdAsBytes);

        Object id = _testAmqpMessage.getCorrelationId();
        assertNotNull("expected underlying correlationId value to be set", id);
        assertTrue("unexpected type of underlying correlation-id object", id instanceof ByteBuffer);
        ByteBuffer buf = (ByteBuffer) id;
        assertNotSame("expected byte arrays to be different objects", providedJmsCorrelationIdAsBytes, buf.array());
    }

    /**
     * Test that the setting a JMSCorrelationID value via {@link MessageImpl#getJMSCorrelationIDAsBytes()}
     * results in a copy of the bytes being set on the underlying message, i.e. not the value provided.
     */
    @Test
    public void testSetJMSCorrelationIDAsBytesToNullOnRecievedMessageWithBinaryIdClearsValue() throws Exception
    {
        String baseId = "myBaseId";
        byte[] expectedJmsCorrelationIdAsBytes = baseId.getBytes();

        ByteBuffer binary = ByteBuffer.wrap(expectedJmsCorrelationIdAsBytes);
        _testAmqpMessage.setCorrelationId(binary);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        //check setting null clears the underlying value
        assertNotNull("expected underlying correlationId value to be set", _testAmqpMessage.getCorrelationId());
        _testMessage.setJMSCorrelationIDAsBytes(null);
        assertNull("expected underlying correlationId value to be cleared", _testAmqpMessage.getCorrelationId());

        //check the JMS message returns null
        assertNull("expected correlationId value to be cleared", _testMessage.getJMSCorrelationIDAsBytes());
    }

    // ====== JMSType =======

    @Test
    public void testGetJMSTypeIsNullOnNewMessage() throws Exception
    {
        assertNull("did not expect a JMSType value to be present", _testMessage.getJMSType());
    }

    /**
     * Test that {@link MessageImpl#setJMSType(String)} sets the expected message
     * annotation on the underlying message to the given value
     */
    @Test
    public void testSetJMSTypeSetsUnderlyingMessageAnnotation() throws Exception
    {
        String jmsType = "myJJMSType";

        _testMessage.setJMSType(jmsType);

        assertTrue("MessageAnnotation should exist to hold JMSType value",
                        _testAmqpMessage.messageAnnotationExists(ClientProperties.X_OPT_JMS_TYPE));
        assertEquals("MessageAnnotation should be set to the provded JMSType string", jmsType,
                        _testAmqpMessage.getMessageAnnotation(ClientProperties.X_OPT_JMS_TYPE));
    }

    /**
     * Test that {@link MessageImpl#setJMSType(String)} sets the expected message
     * annotation on the underlying message to the given value
     */
    @Test
    public void testSetGetJMSTypeOnNewMessage() throws Exception
    {
        String jmsType = "myJJMSType";

        _testMessage.setJMSType(jmsType);

        assertEquals("JMSType value was not as expected", jmsType, _testMessage.getJMSType());
    }

    /**
     * Test that {@link MessageImpl#setJMSType(String)} sets the expected message
     * annotation on the underlying message to the given value
     */
    @Test
    public void testSetJMSTypeNullClearsExistingValue() throws Exception
    {
        String jmsType = "myJJMSType";

        _testMessage.setJMSType(jmsType);
        _testMessage.setJMSType(null);
        assertNull("JMSType value was not as expected", _testMessage.getJMSType());
    }

    /**
     * Test that {@link MessageImpl#getJMSType()} returns the expected value for a message
     * received with the {@link ClientProperties#X_OPT_JMS_TYPE} message annotation set.
     */
    @Test
    public void testGetJMSTypeWithReceivedMessage() throws Exception
    {
        String myJMSType = "myJMSType";
        _testAmqpMessage.setMessageAnnotation(ClientProperties.X_OPT_JMS_TYPE, myJMSType);

        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        assertEquals("JMSType value was not as expected", myJMSType, _testMessage.getJMSType());
    }

    // ====== JMS_AMQP_TTL property =======

    @Test
    public void testSetJMS_AMQP_TTL_PropertyRejectsNegatives() throws Exception
    {
        //check negatives are rejected
        try
        {
            _testMessage.setLongProperty(JMS_AMQP_TTL, -1L);
            fail("expected exception not thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }

        //check values over 2^32 - 1 are rejected
        try
        {
            _testMessage.setLongProperty(JMS_AMQP_TTL, 0X100000000L);
            fail("expected exception not thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    @Test
    public void testSetJMS_AMQP_TTL_PropertyRejectsNonLongValue() throws Exception
    {
        //check an int is rejected
        try
        {
            _testMessage.setIntProperty(JMS_AMQP_TTL, 1);
            fail("expected exception not thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    @Test
    public void testJMS_AMQP_TTL_PropertyExists() throws Exception
    {
        assertFalse(_testMessage.propertyExists(JMS_AMQP_TTL));
        _testMessage.setLongProperty(JMS_AMQP_TTL, 1L);
        assertTrue(_testMessage.propertyExists(JMS_AMQP_TTL));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testJMS_AMQP_TTL_GetPropertyNamesOnNewMessage() throws Exception
    {
        //verify the name doesn't exist originally
        Enumeration<String> names = (Enumeration<String>) _testMessage.getPropertyNames();
        boolean containsJMS_AMQP_TTL = false;
        while(names.hasMoreElements())
        {
            String prop = names.nextElement();
            if(JMS_AMQP_TTL.equals(prop))
            {
                containsJMS_AMQP_TTL = true;
            }
        }

        assertFalse(containsJMS_AMQP_TTL);

        //set property
        _testMessage.setLongProperty(JMS_AMQP_TTL, 1L);

        //verify the namenow exists
        names = (Enumeration<String>) _testMessage.getPropertyNames();
        while(names.hasMoreElements())
        {
            String prop = names.nextElement();
            if(JMS_AMQP_TTL.equals(prop))
            {
                containsJMS_AMQP_TTL = true;
            }
        }

        assertTrue(containsJMS_AMQP_TTL);
    }

    @Test
    public void testGetJMS_AMQP_TTL_PropertyOnNewMessage() throws Exception
    {
        try
        {
            _testMessage.getLongProperty(JMS_AMQP_TTL);
            fail("expected exception not thrown");
        }
        catch(NumberFormatException nfe)
        {
            //expected, property isn't set, so it returns null, and so
            //the null->Long conversion process failure applies
        }
    }

    @Test
    public void testJMS_AMQP_TTL_PropertyBehaviourOnReceivedMessageWithUnderlyingTtlFieldSet() throws Exception
    {
        _testAmqpMessage.setTtl(5L);
        _testMessage = TestMessageImpl.createReceivedMessage(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl, null);

        //check the propertyExists method
        assertFalse(_testMessage.propertyExists(JMS_AMQP_TTL));

        //verify the name doesn't exist
        @SuppressWarnings("unchecked")
        Enumeration<String> names = (Enumeration<String>) _testMessage.getPropertyNames();
        boolean containsJMS_AMQP_TTL = false;
        while(names.hasMoreElements())
        {
            String prop = names.nextElement();
            if(JMS_AMQP_TTL.equals(prop))
            {
                containsJMS_AMQP_TTL = true;
            }
        }
        assertFalse(containsJMS_AMQP_TTL);

        //verify getting the value returns a NFE
        try
        {
            _testMessage.getLongProperty(JMS_AMQP_TTL);
            fail("expected exception not thrown");
        }
        catch(NumberFormatException nfe)
        {
            //expected, property isn't set, so it returns null, and so
            //the null->Long conversion process failure applies
        }
    }

    @Test
    public void testSetJMS_AMQP_TTL_PropertyDoesntSetEntryInUnderlyingApplicationProperties() throws Exception
    {
        assertFalse(_testAmqpMessage.applicationPropertyExists(JMS_AMQP_TTL));

        _testMessage.setLongProperty(JMS_AMQP_TTL, 5L);

        //verify that the underlying message doesn't have a JMS_AMQP_TTL entry in its
        //application-properties section, as we don't transmit the value that way
        assertFalse(_testAmqpMessage.applicationPropertyExists(JMS_AMQP_TTL));
    }

    @Test
    public void testJMS_AMQP_TTL_AfterClearProperties() throws Exception
    {
        _testMessage.setLongProperty(JMS_AMQP_TTL, 5L);

        assertTrue(_testMessage.propertyExists(JMS_AMQP_TTL));

        _testMessage.clearProperties();

        //check the prop no longer exists
        assertFalse(_testMessage.propertyExists(JMS_AMQP_TTL));

        //verify getting the value returns a NFE
        try
        {
            _testMessage.getLongProperty(JMS_AMQP_TTL);
            fail("expected exception not thrown");
        }
        catch(NumberFormatException nfe)
        {
            //expected, property isn't set, so it returns null, and so
            //the null->Long conversion process failure applies
        }

        //check there are no properties
        Enumeration<?> names = _testMessage.getPropertyNames();
        int numProps = 0;
        while(names.hasMoreElements())
        {
            numProps++;
        }

        assertEquals(0, numProps);
    }

    // ====== utility methods =======

    private void assertGetPropertyThrowsMessageFormatException(MessageImpl<?> testMessage,
                                                               String propertyName,
                                                               Class<?> clazz) throws JMSException
    {
        try
        {
            getMessagePropertyUsingTypeMethod(testMessage, propertyName, clazz);

            fail("expected exception to be thrown");
        }
        catch(MessageFormatException jmsMFE)
        {
            //expected
        }
    }

    private Object getMessagePropertyUsingTypeMethod(MessageImpl<?> testMessage, String propertyName, Class<?> clazz) throws JMSException
    {
        if(clazz == Boolean.class)
        {
            return testMessage.getBooleanProperty(propertyName);
        }
        else if(clazz == Byte.class)
        {
            return testMessage.getByteProperty(propertyName);
        }
        else if(clazz == Short.class)
        {
            return testMessage.getShortProperty(propertyName);
        }
        else if(clazz == Integer.class)
        {
            return testMessage.getIntProperty(propertyName);
        }
        else if(clazz == Long.class)
        {
            return testMessage.getLongProperty(propertyName);
        }
        else if(clazz == Float.class)
        {
            return testMessage.getFloatProperty(propertyName);
        }
        else if(clazz == Double.class)
        {
            return testMessage.getDoubleProperty(propertyName);
        }
        else if(clazz == String.class)
        {
            return testMessage.getStringProperty(propertyName);
        }
        else
        {
          throw new RuntimeException("Unexpected property type class");
        }
    }

    private void assertGetPropertyEquals(MessageImpl<?> testMessage,
                                         String propertyName,
                                         Object expectedValue,
                                         Class<?> clazz) throws JMSException
    {
        Object actualValue = getMessagePropertyUsingTypeMethod(testMessage, propertyName, clazz);
        assertEquals(expectedValue, actualValue);
    }
}
