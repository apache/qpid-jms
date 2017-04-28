/*
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
 */
package org.apache.qpid.jms.producer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.Queue;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsContext;
import org.apache.qpid.jms.JmsMessageProducer;
import org.apache.qpid.jms.JmsProducer;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.JmsTemporaryQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.provider.mock.MockRemotePeer;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * Test various behaviors of the JMSProducer implementation.
 */
public class JmsProducerTest extends JmsConnectionTestSupport {

    private JmsContext context;

    private final MockRemotePeer remotePeer = new MockRemotePeer();

    private final String STRING_PROPERTY_NAME = "StringProperty";
    private final String STRING_PROPERTY_VALUE = UUID.randomUUID().toString();

    private final String BYTE_PROPERTY_NAME = "ByteProperty";
    private final byte BYTE_PROPERTY_VALUE = Byte.MAX_VALUE;

    private final String BOOLEAN_PROPERTY_NAME = "BooleanProperty";
    private final boolean BOOLEAN_PROPERTY_VALUE = Boolean.TRUE;

    private final String SHORT_PROPERTY_NAME = "ShortProperty";
    private final short SHORT_PROPERTY_VALUE = Short.MAX_VALUE;

    private final String INTEGER_PROPERTY_NAME = "IntegerProperty";
    private final int INTEGER_PROPERTY_VALUE = Integer.MAX_VALUE;

    private final String LONG_PROPERTY_NAME = "LongProperty";
    private final long LONG_PROPERTY_VALUE = Long.MAX_VALUE;

    private final String DOUBLE_PROPERTY_NAME = "DoubleProperty";
    private final double DOUBLE_PROPERTY_VALUE = Double.MAX_VALUE;

    private final String FLOAT_PROPERTY_NAME = "FloatProperty";
    private final float FLOAT_PROPERTY_VALUE = Float.MAX_VALUE;

    private final String BAD_PROPERTY_NAME = "%_BAD_PROPERTY_NAME";
    private final String GOOD_PROPERTY_NAME = "GOOD_PROPERTY_NAME";

    private final Destination JMS_DESTINATION = new JmsTopic("test.target.topic:001");
    private final Destination JMS_REPLY_TO = new JmsTopic("test.replyto.topic:001");
    private final String JMS_CORRELATION_ID = UUID.randomUUID().toString();
    private final String JMS_TYPE_STRING = "TestType";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        remotePeer.start();
        context = createJMSContextToMockProvider();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            context.close();
        } finally {
            super.tearDown();
        }
    }

    //----- Test Property Handling methods -----------------------------------//

    @Test
    public void testGetPropertyNames() {
        JMSProducer producer = context.createProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        assertTrue(producer.getPropertyNames().contains("Property_1"));
        assertTrue(producer.getPropertyNames().contains("Property_2"));
        assertTrue(producer.getPropertyNames().contains("Property_3"));
    }

    @Test
    public void testClearProperties() {
        JMSProducer producer = context.createProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        producer.clearProperties();

        assertEquals(0, producer.getPropertyNames().size());
    }

    @Test
    public void testPropertyExists() {
        JMSProducer producer = context.createProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        assertTrue(producer.propertyExists("Property_1"));
        assertTrue(producer.propertyExists("Property_2"));
        assertTrue(producer.propertyExists("Property_3"));
        assertFalse(producer.propertyExists("Property_4"));
    }

    //----- Test for JMS Message Headers are stored --------------------------//

    @Test
    public void testJMSCorrelationID() {
        JMSProducer producer = context.createProducer();

        producer.setJMSCorrelationID(JMS_CORRELATION_ID);
        assertEquals(JMS_CORRELATION_ID, producer.getJMSCorrelationID());
    }

    @Test
    public void testJMSCorrelationIDBytes() {
        JMSProducer producer = context.createProducer();

        producer.setJMSCorrelationIDAsBytes(JMS_CORRELATION_ID.getBytes(StandardCharsets.UTF_8));
        assertEquals(JMS_CORRELATION_ID, new String(producer.getJMSCorrelationIDAsBytes(), StandardCharsets.UTF_8));
    }

    @Test
    public void testJMSReplyTo() {
        JMSProducer producer = context.createProducer();

        producer.setJMSReplyTo(JMS_REPLY_TO);
        assertTrue(JMS_REPLY_TO.equals(producer.getJMSReplyTo()));
    }

    @Test
    public void testJMSType() {
        JMSProducer producer = context.createProducer();

        producer.setJMSType(JMS_TYPE_STRING);
        assertEquals(JMS_TYPE_STRING, producer.getJMSType());
    }

    //----- Test for get property on matching types --------------------------//

    @Test
    public void testGetStringPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        assertEquals(STRING_PROPERTY_VALUE, producer.getStringProperty(STRING_PROPERTY_NAME));
    }

    @Test
    public void testGetBytePropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));
    }

    @Test
    public void testGetBooleanPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
    }

    @Test
    public void testGetShortPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));
    }

    @Test
    public void testGetIntegerPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));
    }

    @Test
    public void testGetLongPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));
    }

    @Test
    public void testGetDoublePropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);
    }

    @Test
    public void testGetFloatPropetry() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0f);
    }

    //----- Test for set property handling -----------------------------------//

    @Test
    public void testSetPropertyConversions() {
        JMSProducer producer = context.createProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, Byte.valueOf(BYTE_PROPERTY_VALUE));
        producer.setProperty(BOOLEAN_PROPERTY_NAME, Boolean.valueOf(BOOLEAN_PROPERTY_VALUE));
        producer.setProperty(SHORT_PROPERTY_NAME, Short.valueOf(SHORT_PROPERTY_VALUE));
        producer.setProperty(INTEGER_PROPERTY_NAME, Integer.valueOf(INTEGER_PROPERTY_VALUE));
        producer.setProperty(LONG_PROPERTY_NAME, Long.valueOf(LONG_PROPERTY_VALUE));
        producer.setProperty(FLOAT_PROPERTY_NAME, Float.valueOf(FLOAT_PROPERTY_VALUE));
        producer.setProperty(DOUBLE_PROPERTY_NAME, Double.valueOf(DOUBLE_PROPERTY_VALUE));

        try {
            producer.setProperty(STRING_PROPERTY_NAME, UUID.randomUUID());
            fail("Should not be able to set non-primitive type");
        } catch (MessageFormatRuntimeException mfe) {
        }

        assertNull(producer.getObjectProperty("Unknown"));

        assertEquals(STRING_PROPERTY_VALUE, producer.getStringProperty(STRING_PROPERTY_NAME));
        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0);
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);

        assertEquals(STRING_PROPERTY_VALUE, producer.getObjectProperty(STRING_PROPERTY_NAME));
        assertEquals(BYTE_PROPERTY_VALUE, producer.getObjectProperty(BYTE_PROPERTY_NAME));
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getObjectProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getObjectProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getObjectProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getObjectProperty(LONG_PROPERTY_NAME));
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getObjectProperty(FLOAT_PROPERTY_NAME));
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getObjectProperty(DOUBLE_PROPERTY_NAME));
    }

    //----- Test for get property conversions --------------------------------//

    @Test
    public void testStringPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(String.valueOf(STRING_PROPERTY_VALUE), producer.getStringProperty(STRING_PROPERTY_NAME));
        assertEquals(String.valueOf(BYTE_PROPERTY_VALUE), producer.getStringProperty(BYTE_PROPERTY_NAME));
        assertEquals(String.valueOf(BOOLEAN_PROPERTY_VALUE), producer.getStringProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(String.valueOf(SHORT_PROPERTY_VALUE), producer.getStringProperty(SHORT_PROPERTY_NAME));
        assertEquals(String.valueOf(INTEGER_PROPERTY_VALUE), producer.getStringProperty(INTEGER_PROPERTY_NAME));
        assertEquals(String.valueOf(LONG_PROPERTY_VALUE), producer.getStringProperty(LONG_PROPERTY_NAME));
        assertEquals(String.valueOf(FLOAT_PROPERTY_VALUE), producer.getStringProperty(FLOAT_PROPERTY_NAME));
        assertEquals(String.valueOf(DOUBLE_PROPERTY_VALUE), producer.getStringProperty(DOUBLE_PROPERTY_NAME));
    }

    @Test
    public void testBytePropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));

        try {
            producer.getByteProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getByteProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testBooleanPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(Boolean.FALSE, producer.getBooleanProperty(STRING_PROPERTY_NAME));

        try {
            producer.getBooleanProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testShortPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getShortProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));

        try {
            producer.getShortProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getShortProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testIntPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getIntProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getIntProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));

        try {
            producer.getIntProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getIntProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testLongPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getLongProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getLongProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getLongProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));

        try {
            producer.getLongProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getLongProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getLongProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getLongProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testFloatPropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0f);

        try {
            producer.getFloatProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getFloatProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testDoublePropertyConversions() {
        JMSProducer producer = context.createProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getDoubleProperty(FLOAT_PROPERTY_NAME), 0.0);

        try {
            producer.getDoubleProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getDoubleProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    //----- Test for error when set called with invalid name -----------------//

    @Test
    public void testSetStringPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, "X");
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetBytePropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, (byte) 1);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetBooleanPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, true);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetDoublePropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, 100.0);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetFloatPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, 100.0f);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetShortPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, (short) 100);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetIntPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, 100);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetLongPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, 100l);
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetObjectPropetryWithBadPropetyName() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(BAD_PROPERTY_NAME, UUID.randomUUID());
            fail("Should not accept invalid property name");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testSetObjectPropetryWithInvalidObject() {
        JMSProducer producer = context.createProducer();

        try {
            producer.setProperty(GOOD_PROPERTY_NAME, UUID.randomUUID());
            fail("Should not accept invalid property name");
        } catch (MessageFormatRuntimeException mfre) {}
    }

    //----- Tests for producer send configuration methods --------------------//

    @Test
    public void testAsync() {
        JMSProducer producer = context.createProducer();
        TestJmsCompletionListener listener = new TestJmsCompletionListener();

        producer.setAsync(listener);
        assertEquals(listener, producer.getAsync());
    }

    @Test
    public void testDeliveryMode() {
        JMSProducer producer = context.createProducer();

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
    }

    @Test(timeout = 10000)
    public void testDeliveryModeConfigurationWithInvalidMode() throws Exception {
        JMSProducer producer = context.createProducer();

        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());

        try {
            producer.setDeliveryMode(-1);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        try {
            producer.setDeliveryMode(5);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
    }

    @Test
    public void testDeliveryDelay() {
        JMSProducer producer = context.createProducer();

        producer.setDeliveryDelay(2000);
        assertEquals(2000, producer.getDeliveryDelay());
    }

    @Test
    public void testDisableMessageID() {
        JMSProducer producer = context.createProducer();

        producer.setDisableMessageID(false);
        assertEquals(false, producer.getDisableMessageID());
        producer.setDisableMessageID(true);
        assertEquals(true, producer.getDisableMessageID());
    }

    @Test
    public void testMessageDisableTimestamp() {
        JMSProducer producer = context.createProducer();

        producer.setDisableMessageTimestamp(false);
        assertEquals(false, producer.getDisableMessageTimestamp());
        producer.setDisableMessageTimestamp(true);
        assertEquals(true, producer.getDisableMessageTimestamp());
    }

    @Test
    public void testPriority() {
        JMSProducer producer = context.createProducer();

        producer.setPriority(1);
        assertEquals(1, producer.getPriority());
        producer.setPriority(4);
        assertEquals(4, producer.getPriority());
    }

    @Test(timeout = 10000)
    public void testPriorityConfigurationWithInvalidPriorityValues() throws Exception {
        JMSProducer producer = context.createProducer();

        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());

        try {
            producer.setPriority(-1);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        try {
            producer.setPriority(10);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
    }

    @Test
    public void testTimeToLive() {
        JMSProducer producer = context.createProducer();

        producer.setTimeToLive(2000);
        assertEquals(2000, producer.getTimeToLive());
    }

    //----- Test Send Methods -----------------------------------------------//

    @Test
    public void testSendNullMessageThrowsMFRE() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageProducer messageProducer = Mockito.mock(JmsMessageProducer.class);

        JmsProducer producer = new JmsProducer(session, messageProducer);

        try {
            producer.send(JMS_DESTINATION, (Message) null);
            fail("Should throw a MessageFormatRuntimeException");
        } catch (MessageFormatRuntimeException mfre) {
        } catch (Exception e) {
            fail("Should throw a MessageFormatRuntimeException");
        }
    }

    @Test
    public void testSendJMSMessage() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageProducer messageProducer = Mockito.mock(JmsMessageProducer.class);
        Message message = Mockito.mock(Message.class);

        JmsProducer producer = new JmsProducer(session, messageProducer);

        producer.setJMSCorrelationID(JMS_CORRELATION_ID);
        producer.setJMSCorrelationIDAsBytes(JMS_CORRELATION_ID.getBytes());
        producer.setJMSReplyTo(JMS_REPLY_TO);
        producer.setJMSType(JMS_TYPE_STRING);

        producer.send(JMS_DESTINATION, message);

        Mockito.verify(message).setJMSCorrelationID(JMS_CORRELATION_ID);
        Mockito.verify(message).setJMSCorrelationIDAsBytes(JMS_CORRELATION_ID.getBytes());
        Mockito.verify(message).setJMSReplyTo(JMS_REPLY_TO);
        Mockito.verify(message).setJMSType(JMS_TYPE_STRING);
    }

    //----- Test that Send methods modify Message data -----------------------//

    @Test
    public void testSendAppliesDeliveryDelayMessageBody() throws JMSException {
        doTestSendAppliesDeliveryDelayMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesDeliveryDelayStringMessageBody() throws JMSException {
        doTestSendAppliesDeliveryDelayMessageBody(String.class);
    }

    @Test
    public void testSendAppliesDeliveryDelayMapMessageBody() throws JMSException {
        doTestSendAppliesDeliveryDelayMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesDeliveryDelayBytesMessageBody() throws JMSException {
        doTestSendAppliesDeliveryDelayMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesDeliveryDelaySerializableMessageBody() throws JMSException {
        doTestSendAppliesDeliveryDelayMessageBody(UUID.class);
    }

    private void doTestSendAppliesDeliveryDelayMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        // Create matcher to expect the DeliveryTime to be set to a value
        // representing 'now', within a upper delta for execution time.
        long deliveryTimeLower = System.currentTimeMillis();
        long deliveryTimeUpper = deliveryTimeLower + 3000;
        Matcher<Long> inRange = both(greaterThanOrEqualTo(deliveryTimeLower)).and(lessThanOrEqualTo(deliveryTimeUpper));

        sendWithBodyOfType(producer, bodyType);

        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertThat(message.getJMSDeliveryTime(), inRange);

        // Repeat with a non-zero delay
        int deliveryDelay = 123456;
        producer.setDeliveryDelay(deliveryDelay);

        // Create matcher to expect the DeliveryTime to be set to a value
        // representing 'now' + delivery-delay, within a upper delta for execution time.
        deliveryTimeLower = System.currentTimeMillis();
        deliveryTimeUpper = deliveryTimeLower + deliveryDelay + 3000;
        inRange = both(greaterThanOrEqualTo(deliveryTimeLower)).and(lessThanOrEqualTo(deliveryTimeUpper));

        sendWithBodyOfType(producer, bodyType);

        envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        assertThat(message.getJMSDeliveryTime(), inRange);
    }

    @Test
    public void testSendAppliesDeliveryModeMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesDeliveryModeStringMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(String.class);
    }

    @Test
    public void testSendAppliesDeliveryModeMapMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesDeliveryModeBytesMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesDeliveryModeSerializableMessageBody() throws JMSException {
        doTestSendAppliesDeliveryModeWithMessageBody(UUID.class);
    }

    public void doTestSendAppliesDeliveryModeWithMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(JMS_DESTINATION, "text");
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());

        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(JMS_DESTINATION, "text");
        envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());
    }

    @Test
    public void testSendAppliesDisableMessageIDMessageBody() throws JMSException {
        doTestSendAppliesDisableMessageIDWithMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesDisableMessageIDStringMessageBody() throws JMSException {
        doTestSendAppliesDisableMessageIDWithMessageBody(String.class);
    }

    @Test
    public void testSendAppliesDisableMessageIDMapMessageBody() throws JMSException {
        doTestSendAppliesDisableMessageIDWithMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesDisableMessageIDBytesMessageBody() throws JMSException {
        doTestSendAppliesDisableMessageIDWithMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesDisableMessageIDSerializableMessageBody() throws JMSException {
        doTestSendAppliesDisableMessageIDWithMessageBody(UUID.class);
    }

    private void doTestSendAppliesDisableMessageIDWithMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        producer.setDisableMessageID(true);
        producer.send(JMS_DESTINATION, "text");
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertNull(message.getJMSMessageID());

        producer.setDisableMessageID(false);
        producer.send(JMS_DESTINATION, "text");
        envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        assertNotNull(message.getJMSMessageID());
    }

    @Test
    public void testSendAppliesDisableTimestampMessageBody() throws JMSException {
        doTestSendAppliesDisableTimestampWithMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesDisableTimestampStringMessageBody() throws JMSException {
        doTestSendAppliesDisableTimestampWithMessageBody(String.class);
    }

    @Test
    public void testSendAppliesDisableTimestampMapMessageBody() throws JMSException {
        doTestSendAppliesDisableTimestampWithMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesDisableTimestampBytesMessageBody() throws JMSException {
        doTestSendAppliesDisableTimestampWithMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesDisableTimestampSerializableMessageBody() throws JMSException {
        doTestSendAppliesDisableTimestampWithMessageBody(UUID.class);
    }

    private void doTestSendAppliesDisableTimestampWithMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        producer.setDisableMessageTimestamp(true);
        producer.send(JMS_DESTINATION, "text");
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertTrue(message.getJMSTimestamp() == 0);

        producer.setDisableMessageTimestamp(false);
        producer.send(JMS_DESTINATION, "text");
        envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        assertFalse(message.getJMSTimestamp() == 0);
    }

    @Test
    public void testSendAppliesPrioirtyMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesPriorityStringMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(String.class);
    }

    @Test
    public void testSendAppliesPriorityMapMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesPriorityBytesMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesPrioritySerializableMessageBody() throws JMSException {
        doTestSendAppliesPriorityWithMessageBody(UUID.class);
    }

    private void doTestSendAppliesPriorityWithMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        producer.setPriority(0);
        producer.send(JMS_DESTINATION, "text");
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertEquals(0, message.getJMSPriority());

        producer.setPriority(7);
        producer.send(JMS_DESTINATION, "text");
        envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        assertEquals(7, message.getJMSPriority());
    }

    @Test
    public void testSendAppliesTimeToLiveMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(Message.class);
    }

    @Test
    public void testSendAppliesTimeToLiveStringMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(String.class);
    }

    @Test
    public void testSendAppliesTimeToLiveMapMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(Map.class);
    }

    @Test
    public void testSendAppliesTimeToLiveBytesMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(byte[].class);
    }

    @Test
    public void testSendAppliesTimeToLiveSerializableMessageBody() throws JMSException {
        doTestSendAppliesTimeToLiveWithMessageBody(UUID.class);
    }

    private void doTestSendAppliesTimeToLiveWithMessageBody(Class<?> bodyType) throws JMSException {
        JMSProducer producer = context.createProducer();

        producer.setTimeToLive(2000);
        producer.send(JMS_DESTINATION, "text");
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertTrue(message.getJMSExpiration() > 0);

        producer.setTimeToLive(Message.DEFAULT_TIME_TO_LIVE);
        producer.send(JMS_DESTINATION, "text");
        envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        message = envelope.getMessage();
        assertTrue(message.getJMSExpiration() == 0);
    }

    //----- Test that body values are written into sent messages -------------//

    @Test
    public void testStringBodyIsApplied() throws JMSException {
        JMSProducer producer = context.createProducer();

        final String bodyValue = "String-Value";

        producer.send(JMS_DESTINATION, bodyValue);
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertEquals(bodyValue, message.getBody(String.class));
    }

    @Test
    public void testMapBodyIsApplied() throws JMSException {
        JMSProducer producer = context.createProducer();

        final Map<String, Object> bodyValue = new HashMap<String, Object>();

        bodyValue.put("Value-1", "First");
        bodyValue.put("Value-2", "Second");

        producer.send(JMS_DESTINATION, bodyValue);
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertEquals(bodyValue, message.getBody(Map.class));
    }

    @Test
    public void testBytesBodyIsApplied() throws JMSException {
        JMSProducer producer = context.createProducer();

        final byte[] bodyValue = new byte[] { 0, 1, 2, 3, 4 };

        producer.send(JMS_DESTINATION, bodyValue);
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();

        byte[] payload = message.getBody(byte[].class);
        assertNotNull(payload);
        assertEquals(bodyValue.length, payload.length);

        for (int i = 0; i < payload.length; ++i) {
            assertEquals(bodyValue[i], payload[i]);
        }
    }

    @Test
    public void testSerializableBodyIsApplied() throws JMSException {
        JMSProducer producer = context.createProducer();

        final UUID bodyValue = UUID.randomUUID();

        producer.send(JMS_DESTINATION, bodyValue);
        JmsOutboundMessageDispatch envelope = remotePeer.getLastReceivedMessage();
        assertNotNull(envelope);
        JmsMessage message = envelope.getMessage();
        assertEquals(bodyValue, message.getBody(UUID.class));
    }

    //----- Test for conversions to JMSRuntimeException ----------------------//

    @Test
    public void testRuntimeExceptionFromSendMessage() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageProducer messageProducer = Mockito.mock(JmsMessageProducer.class);
        Message message = Mockito.mock(Message.class);

        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(session.createMessage()).thenReturn(message);

        Mockito.doThrow(IllegalStateException.class).when(message).setJMSCorrelationID(Matchers.anyString());

        JmsProducer producer = new JmsProducer(session, messageProducer);

        producer.setJMSCorrelationID("id");

        try {
            producer.send(session.createTemporaryQueue(), session.createMessage());
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendByteBody() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageProducer messageProducer = Mockito.mock(JmsMessageProducer.class);

        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(session.createMessage()).thenReturn(Mockito.mock(Message.class));

        Mockito.doThrow(IllegalStateException.class).when(session).createBytesMessage();

        JmsProducer producer = new JmsProducer(session, messageProducer);

        try {
            producer.send(session.createTemporaryQueue(), new byte[0]);
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendMapBody() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageProducer messageProducer = Mockito.mock(JmsMessageProducer.class);

        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(session.createMessage()).thenReturn(Mockito.mock(Message.class));

        Mockito.doThrow(IllegalStateException.class).when(session).createMapMessage();

        JmsProducer producer = new JmsProducer(session, messageProducer);

        try {
            producer.send(session.createTemporaryQueue(), Collections.<String, Object>emptyMap());
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendSerializableBody() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageProducer messageProducer = Mockito.mock(JmsMessageProducer.class);

        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(session.createMessage()).thenReturn(Mockito.mock(Message.class));

        Mockito.doThrow(IllegalStateException.class).when(session).createObjectMessage();

        JmsProducer producer = new JmsProducer(session, messageProducer);

        try {
            producer.send(session.createTemporaryQueue(), UUID.randomUUID());
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendStringBody() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageProducer messageProducer = Mockito.mock(JmsMessageProducer.class);

        Mockito.when(session.createTemporaryQueue()).thenReturn(new JmsTemporaryQueue());
        Mockito.when(session.createMessage()).thenReturn(Mockito.mock(Message.class));

        Mockito.doThrow(IllegalStateException.class).when(session).createTextMessage(Matchers.anyString());

        JmsProducer producer = new JmsProducer(session, messageProducer);

        try {
            producer.send(session.createTemporaryQueue(), "test");
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSetJMSReplyTo() throws JMSException {
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsMessageProducer messageProducer = Mockito.mock(JmsMessageProducer.class);
        Queue queue = Mockito.mock(Queue.class);

        Mockito.doThrow(IllegalStateException.class).when(queue).getQueueName();

        JmsProducer producer = new JmsProducer(session, messageProducer);

        try {
            producer.setJMSReplyTo(queue);
            fail("Should have thrown an exception");
        } catch (IllegalStateRuntimeException isre) {}
    }

    @Test
    public void testRuntimeExceptionFromSendToInvalidDestination() throws JMSException {
        class TestJmsSession extends JmsSession {
            protected TestJmsSession(JmsConnection connection, JmsSessionId sessionId, int acknowledgementMode) throws JMSException {
                super(connection, sessionId, acknowledgementMode);
            }

            @Override
            public void send(JmsMessageProducer producer, Destination dest, Message msg, int deliveryMode, int priority, long timeToLive,
                             boolean disableMsgId, boolean disableTimestamp, long deliveryDelay, CompletionListener listener) throws JMSException {
                super.send(producer, dest, msg, deliveryMode, priority, timeToLive, disableMsgId, disableTimestamp, deliveryDelay, listener);
            }
        };

        JmsMessageProducer mockMessageProducer = Mockito.mock(JmsMessageProducer.class);
        TestJmsSession mockSession = Mockito.mock(TestJmsSession.class);

        Mockito.doThrow(new InvalidDestinationException("ide"))
                .when(mockSession)
                .send(Mockito.any(JmsMessageProducer.class), Mockito.any(Destination.class), Mockito.any(Message.class),
                      Mockito.any(int.class), Mockito.any(int.class), Mockito.any(long.class), Mockito.any(boolean.class),
                      Mockito.any(boolean.class), Mockito.any(long.class), Mockito.any(CompletionListener.class));

        JmsProducer producer = new JmsProducer(mockSession, mockMessageProducer);

        try {
            producer.send(null, Mockito.mock(Message.class));
            fail("Should have thrown an InvalidDestinationRuntimeException");
        } catch (InvalidDestinationRuntimeException idre) {}

        Mockito.verify(mockSession).send(Mockito.any(JmsMessageProducer.class), Mockito.any(Destination.class), Mockito.any(Message.class),
                Mockito.any(int.class), Mockito.any(int.class), Mockito.any(long.class), Mockito.any(boolean.class),
                Mockito.any(boolean.class), Mockito.any(long.class), Mockito.any(CompletionListener.class));
    }

    //----- Internal Support -------------------------------------------------//

    private void sendWithBodyOfType(JMSProducer producer, Class<?> asType) {
        if (asType.equals(String.class)) {
            producer.send(JMS_DESTINATION, "String-body-type");
        } else if (asType.equals(Map.class)) {
            producer.send(JMS_DESTINATION, Collections.<String, Object>emptyMap());
        } else if (asType.equals(Message.class)) {
            producer.send(JMS_DESTINATION, context.createMessage());
        } else if (asType.equals(byte[].class)) {
            producer.send(JMS_DESTINATION, new byte[10]);
        } else if (asType.equals(UUID.class)) {
            producer.send(JMS_DESTINATION, UUID.randomUUID());
        }
    }

    private class TestJmsCompletionListener implements CompletionListener {

        @Override
        public void onCompletion(Message message) {
        }

        @Override
        public void onException(Message message, Exception exception) {
        }
    }
}
