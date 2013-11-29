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

import static org.junit.Assert.*;

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.engine.TestAmqpMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MessageImplTest extends QpidJmsTestCase
{
    private ConnectionImpl _mockConnectionImpl;
    private SessionImpl _mockSessionImpl;
    private TestMessageImpl _testMessage;
    private TestAmqpMessage _testAmqpMessage;
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
        _testAmqpMessage = new TestAmqpMessage();
        _testMessage = new TestMessageImpl(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl);

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
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());

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
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());

        assertNull(_testAmqpMessage.getTo());

        _testMessage.setJMSDestination(_mockTopic);

        assertNotNull(_testAmqpMessage.getTo());
        assertEquals(_mockTopicName, _testAmqpMessage.getTo());

        assertTrue(_testAmqpMessage.messageAnnotationExists(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
        assertEquals(DestinationHelper.TOPIC_ATTRIBUTES_STRING,
                     _testAmqpMessage.getMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
    }

    @Test
    public void testSetJMSDestinationNullOnRecievedMessageWithToAndTypeAnnotationClearsTheAnnotation() throws Exception
    {
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
        _testAmqpMessage.setTo(_mockTopicName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.TOPIC_ATTRIBUTES_STRING);
        _testMessage = new TestMessageImpl(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl);

        assertNotNull("expected JMSDestination value not present", _testMessage.getJMSDestination());
        assertTrue(_testAmqpMessage.messageAnnotationExists(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));

        _testMessage.setJMSDestination(null);

        assertNull("expected JMSDestination value to be null", _testMessage.getJMSDestination());
        assertFalse(_testAmqpMessage.messageAnnotationExists(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME));
    }

    @Test
    public void testSetGetJMSDestinationOnNewMessage() throws Exception
    {
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
        _testMessage.setJMSDestination(_mockQueue);
        assertNotNull(_testMessage.getJMSDestination());
        assertSame(_mockQueue, _testMessage.getJMSDestination());
    }

    @Test
    public void testGetJMSDestinationOnRecievedMessageWithToButWithoutToTypeAnnotation() throws Exception
    {
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
        _testAmqpMessage.setTo(_mockQueueName);
        _testMessage = new TestMessageImpl(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl);

        assertNotNull("expected JMSDestination value not present", _testMessage.getJMSDestination());

        Destination newDestinationExpected = new DestinationImpl(_mockQueueName);
        assertEquals(newDestinationExpected, _testMessage.getJMSDestination());
    }

    @Test
    public void testGetJMSDestinationOnRecievedMessageWithToAndTypeAnnotationForTopic() throws Exception
    {
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
        _testAmqpMessage.setTo(_mockTopicName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.TOPIC_ATTRIBUTES_STRING);
        _testMessage = new TestMessageImpl(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl);

        assertNotNull("expected JMSDestination value not present", _testMessage.getJMSDestination());

        Topic newDestinationExpected = new DestinationHelper().createTopic(_mockTopicName);
        assertEquals(newDestinationExpected, _testMessage.getJMSDestination());
    }

    @Test
    public void testGetJMSDestinationOnRecievedMessageWithToAndTypeAnnotationForQueue() throws Exception
    {
        Mockito.when(_mockSessionImpl.getDestinationHelper()).thenReturn(new DestinationHelper());
        _testAmqpMessage.setTo(_mockQueueName);
        _testAmqpMessage.setMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME,
                                              DestinationHelper.QUEUE_ATTRIBUTES_STRING);
        _testMessage = new TestMessageImpl(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl);

        assertNotNull("expected JMSDestination value not present", _testMessage.getJMSDestination());

        Queue newDestinationExpected = new DestinationHelper().createQueue(_mockQueueName);
        assertEquals(newDestinationExpected, _testMessage.getJMSDestination());
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
        assertEquals(0, _testAmqpMessage.getCreationTime());

        long timestamp = System.currentTimeMillis();
        _testMessage.setJMSTimestamp(timestamp);

        assertEquals(timestamp, _testAmqpMessage.getCreationTime());
    }

    @Test
    public void testGetJMSTimestampOnRecievedMessageWithCreationTime() throws Exception
    {
        long timestamp = System.currentTimeMillis();
        _testAmqpMessage.setCreationTime(timestamp);
        _testMessage = new TestMessageImpl(_testAmqpMessage, _mockSessionImpl, _mockConnectionImpl);

        assertEquals("expected JMSTimestamp value not present", timestamp, _testMessage.getJMSTimestamp());
    }

    // ====== utility methods =======

    private void assertGetPropertyThrowsMessageFormatException(TestMessageImpl testMessage,
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

    private Object getMessagePropertyUsingTypeMethod(TestMessageImpl testMessage, String propertyName, Class<?> clazz) throws JMSException
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

    private void assertGetPropertyEquals(TestMessageImpl testMessage,
                                         String propertyName,
                                         Object expectedValue,
                                         Class<?> clazz) throws JMSException
    {
        Object actualValue = getMessagePropertyUsingTypeMethod(testMessage, propertyName, clazz);
        assertEquals(expectedValue, actualValue);
    }
}
