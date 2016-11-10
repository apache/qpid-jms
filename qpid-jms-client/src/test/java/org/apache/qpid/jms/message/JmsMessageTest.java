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

import static org.apache.qpid.jms.message.JmsMessageSupport.ACCEPTED;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_AMQP_ACK_TYPE;
import static org.apache.qpid.jms.message.JmsMessageSupport.RELEASED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Map;
import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsAcknowledgeCallback;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFacade;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsMessageTest {

    private static final Logger LOG = LoggerFactory.getLogger(JmsMessageTest.class);

    private final JmsMessageFactory factory = new JmsTestMessageFactory();

    private String jmsMessageID;
    private String jmsCorrelationID;
    private JmsDestination jmsDestination;
    private JmsDestination jmsReplyTo;
    private int jmsDeliveryMode;
    private boolean jmsRedelivered;
    private String jmsType;
    private long jmsExpiration;
    private int jmsPriority;
    private long jmsTimestamp;
    private long[] consumerIDs;

    @Before
    public void setUp() throws Exception {
        this.jmsMessageID = "ID:TEST-ID:0:0:0:1";
        this.jmsCorrelationID = "testcorrelationid";
        this.jmsDestination = new JmsTopic("test.topic");
        this.jmsReplyTo = new JmsTopic("test.replyto.topic:001");
        this.jmsDeliveryMode = Message.DEFAULT_DELIVERY_MODE;
        this.jmsRedelivered = true;
        this.jmsType = "test type";
        this.jmsExpiration = 100000;
        this.jmsPriority = 5;
        this.jmsTimestamp = System.currentTimeMillis();
        this.consumerIDs = new long[3];
        for (int i = 0; i < this.consumerIDs.length; i++) {
            this.consumerIDs[i] = i;
        }
    }

    @Test
    public void testMessageSetToReadOnlyOnSend() throws Exception {
        JmsMessage msg = factory.createMessage();
        assertFalse(msg.isReadOnlyBody());
        assertFalse(msg.isReadOnlyProperties());
        msg.onSend(0);
        assertTrue(msg.isReadOnly());
    }

    @Test
    public void testMessageSetToReadOnlyOnDispatch() throws Exception {
        JmsMessage msg = factory.createMessage();
        assertFalse(msg.isReadOnlyBody());
        assertFalse(msg.isReadOnlyProperties());
        msg.onDispatch();
        assertTrue(msg.isReadOnlyBody());
        assertTrue(msg.isReadOnlyProperties());
    }

    @Test
    public void testToString() throws Exception {
        JmsMessage msg = factory.createMessage();
        assertTrue(msg.toString().startsWith("JmsMessage"));
    }

    @Test
    public void testHashCode() throws Exception {
        JmsMessage msg = factory.createMessage();
        msg.setJMSMessageID(this.jmsMessageID);
        assertEquals(msg.getJMSMessageID().hashCode(), jmsMessageID.hashCode());
        assertEquals(msg.hashCode(), jmsMessageID.hashCode());
    }

    @Test
    public void testHashCodeWhenNoMessageIDAssigned() throws Exception {
        JmsMessage msg1 = factory.createMessage();
        JmsMessage msg2 = factory.createMessage();

        assertFalse(msg1.hashCode() == msg2.hashCode());
        assertTrue(msg1.hashCode() == msg1.hashCode());
    }

    @Test
    public void testSetReadOnly() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setReadOnlyProperties(true);
        boolean test = false;
        try {
            msg.setIntProperty("test", 1);
        } catch (MessageNotWriteableException me) {
            test = true;
        } catch (JMSException e) {
            e.printStackTrace(System.err);
            test = false;
        }
        assertTrue(test);
    }

    @Test
    public void testSetToForeignJMSID() throws Exception {
        JmsMessage msg = factory.createMessage();
        msg.setJMSMessageID("ID:EMS-SERVER.8B443C380083:429");
    }

    @Test
    public void testEqualsObject() throws Exception {
        JmsMessage msg1 = factory.createMessage();
        JmsMessage msg2 = factory.createMessage();
        msg1.setJMSMessageID(this.jmsMessageID);
        assertTrue(!msg1.equals(msg2));
        assertTrue(!msg2.equals(msg1));
        msg2.setJMSMessageID(this.jmsMessageID);
        assertTrue(msg1.equals(msg2));
        assertTrue(msg2.equals(msg1));
        msg2.setJMSMessageID(this.jmsMessageID + "More");
        assertTrue(!msg1.equals(msg2));
        assertTrue(!msg2.equals(msg1));

        assertTrue(msg1.equals(msg1));
        assertFalse(msg1.equals(null));
        assertFalse(msg1.equals(""));
    }

    @Test
    public void testEqualsObjectNullMessageIds() throws Exception {
        JmsMessage msg1 = factory.createMessage();
        JmsMessage msg2 = factory.createMessage();
        assertFalse(msg2.equals(msg1));
        assertFalse(msg1.equals(msg2));
    }

    @Test
    public void testShallowCopy() throws Exception {
        JmsMessage msg1 = factory.createMessage();
        msg1.setJMSMessageID(jmsMessageID);
        JmsMessage msg2 = msg1.copy();
        assertTrue(msg1 != msg2 && msg1.equals(msg2));
    }

    @Test
    public void testCopy() throws Exception {
        this.jmsMessageID = "testid";
        this.jmsCorrelationID = "testcorrelationid";
        this.jmsDestination = new JmsTopic("test.topic");
        this.jmsReplyTo = new JmsTopic("test.replyto.topic:001");
        this.jmsDeliveryMode = Message.DEFAULT_DELIVERY_MODE;
        this.jmsRedelivered = true;
        this.jmsType = "test type";
        this.jmsExpiration = 100000;
        this.jmsPriority = 5;
        this.jmsTimestamp = System.currentTimeMillis();

        JmsConnection connection = Mockito.mock(JmsConnection.class);

        JmsMessage msg1 = factory.createMessage();
        msg1.setJMSMessageID(this.jmsMessageID);
        msg1.setJMSCorrelationID(this.jmsCorrelationID);
        msg1.setJMSDestination(this.jmsDestination);
        msg1.setJMSReplyTo(this.jmsReplyTo);
        msg1.setJMSDeliveryMode(this.jmsDeliveryMode);
        msg1.setJMSRedelivered(this.jmsRedelivered);
        msg1.setJMSType(this.jmsType);
        msg1.setJMSExpiration(this.jmsExpiration);
        msg1.setJMSPriority(this.jmsPriority);
        msg1.setJMSTimestamp(this.jmsTimestamp);
        msg1.setReadOnlyProperties(true);
        msg1.setConnection(connection);
        boolean isValidate = !msg1.isValidatePropertyNames();
        msg1.setValidatePropertyNames(isValidate);
        JmsMessage msg2 = msg1.copy();
        assertEquals(msg1.getJMSMessageID(), msg2.getJMSMessageID());
        assertTrue(msg2.isReadOnlyProperties());
        assertTrue(msg1.getJMSCorrelationID().equals(msg2.getJMSCorrelationID()));
        assertTrue(msg1.getJMSDestination().equals(msg2.getJMSDestination()));
        assertTrue(msg1.getJMSReplyTo().equals(msg2.getJMSReplyTo()));
        assertEquals(msg1.getJMSDeliveryMode(), msg2.getJMSDeliveryMode());
        assertEquals(msg1.getJMSRedelivered(), msg2.getJMSRedelivered());
        assertEquals(msg1.getJMSType(), msg2.getJMSType());
        assertEquals(msg1.getJMSExpiration(), msg2.getJMSExpiration());
        assertEquals(msg1.getJMSPriority(), msg2.getJMSPriority());
        assertEquals(msg1.getJMSTimestamp(), msg2.getJMSTimestamp());
        assertEquals(msg1.getConnection(), msg2.getConnection());
        assertEquals(msg1.isValidatePropertyNames(), msg2.isValidatePropertyNames());

        LOG.info("Message is:  " + msg1);
    }

    @Test
    public void testGetAndSetJMSMessageID() throws Exception {
        JmsMessage msg = factory.createMessage();
        assertNull(msg.getJMSMessageID());
        msg.setJMSMessageID(this.jmsMessageID);
        assertEquals(msg.getJMSMessageID(), this.jmsMessageID);
    }

    @Test
    public void testGetJMSMessageIDAlwaysHasIdPrefix() throws Exception {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        Mockito.when(facade.getMessageId()).thenReturn("123456");
        JmsMessage msg = new JmsMessage(facade);
        assertTrue(msg.getJMSMessageID().startsWith("ID:"));
    }

    @Test
    public void testGetAndSetJMSTimestamp() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setJMSTimestamp(this.jmsTimestamp);
        assertTrue(msg.getJMSTimestamp() == this.jmsTimestamp);
    }

    @Test
    public void testGetJMSCorrelationIDAsBytes() throws Exception {
        JmsMessage msg = factory.createMessage();
        msg.setJMSCorrelationID(this.jmsCorrelationID);
        byte[] testbytes = msg.getJMSCorrelationIDAsBytes();
        String str2 = new String(testbytes);
        assertTrue(this.jmsCorrelationID.equals(str2));
    }

    @Test
    public void testSetJMSCorrelationIDAsBytes() throws Exception {
        JmsMessage msg = factory.createMessage();
        byte[] testbytes = this.jmsCorrelationID.getBytes();
        msg.setJMSCorrelationIDAsBytes(testbytes);
        testbytes = msg.getJMSCorrelationIDAsBytes();
        String str2 = new String(testbytes);
        assertTrue(this.jmsCorrelationID.equals(str2));
    }

    @Test
    public void testGetAndSetJMSCorrelationID() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setJMSCorrelationID(this.jmsCorrelationID);
        assertTrue(msg.getJMSCorrelationID().equals(this.jmsCorrelationID));
    }

    @Test
    public void testGetAndSetJMSReplyTo() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setJMSReplyTo(this.jmsReplyTo);
        assertTrue(msg.getJMSReplyTo().equals(this.jmsReplyTo));
    }

    @Test
    public void testGetAndSetJMSDestination() throws Exception {
        JmsMessage msg = factory.createMessage();
        msg.setJMSDestination(this.jmsDestination);
        assertTrue(msg.getJMSDestination().equals(this.jmsDestination));
    }

    @Test
    public void testGetAndSetJMSDeliveryMode() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setJMSDeliveryMode(this.jmsDeliveryMode);
        assertTrue(msg.getJMSDeliveryMode() == this.jmsDeliveryMode);
        msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, msg.getJMSDeliveryMode());
        msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
        assertEquals(DeliveryMode.PERSISTENT, msg.getJMSDeliveryMode());
    }

    @Test
    public void testSetJMSDeliveryModeWithInvalidValue() throws JMSException {
        JmsMessage msg = factory.createMessage();
        assertEquals(Message.DEFAULT_DELIVERY_MODE, msg.getJMSDeliveryMode());
        try {
            msg.setJMSDeliveryMode(-1);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception: {}", ex.getMessage());
        }
        try {
            msg.setJMSDeliveryMode(3);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception: {}", ex.getMessage());
        }
        assertEquals(Message.DEFAULT_DELIVERY_MODE, msg.getJMSDeliveryMode());
    }

    @Test
    public void testGetAndSetMSRedelivered() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setJMSRedelivered(this.jmsRedelivered);
        assertTrue(msg.getJMSRedelivered() == this.jmsRedelivered);
    }

    @Test
    public void testGetAndSetJMSType() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setJMSType(this.jmsType);
        assertTrue(msg.getJMSType().equals(this.jmsType));
    }

    @Test
    public void testGetAndSetJMSExpiration() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setJMSExpiration(this.jmsExpiration);
        assertTrue(msg.getJMSExpiration() == this.jmsExpiration);
    }

    @Test
    public void testGetAndSetJMSPriority() throws JMSException {
        JmsMessage message = factory.createMessage();
        assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());

        try {
            message.setJMSPriority(-1);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception: {}", ex.getMessage());
        }
        try {
            message.setJMSPriority(10);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception: {}", ex.getMessage());
        }

        assertEquals(Message.DEFAULT_PRIORITY, message.getJMSPriority());
    }

    @Test
    public void testClearProperties() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setStringProperty("test", "test");
        msg.setJMSMessageID(this.jmsMessageID);
        msg.clearProperties();
        assertNull(msg.getStringProperty("test"));
        assertNotNull(msg.getJMSMessageID());
    }

    @Test
    public void testClearPropertiesClearsReadOnly() throws Exception {
        JmsMessage msg = factory.createMessage();
        msg.onDispatch();

        try {
            msg.setObjectProperty("test", "value");
            fail("should throw exception");
        } catch (MessageNotWriteableException e) {
            // Expected
        }

        assertTrue(msg.isReadOnlyProperties());

        msg.clearProperties();

        msg.setObjectProperty("test", "value");

        assertFalse(msg.isReadOnlyProperties());
    }

    @Test
    public void testClearPropertiesClearsFacadeGroupSequence() throws JMSException {
        JmsMessageFacade facade = Mockito.mock(JmsMessageFacade.class);
        JmsMessage msg = new JmsMessage(facade);
        msg.clearProperties();
        Mockito.verify(facade).setGroupSequence(0);
    }

    @Test
    public void testPropertyExists() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setStringProperty("test", "test");
        assertTrue(msg.propertyExists("test"));

        msg.setIntProperty("JMSXDeliveryCount", 1);
        assertTrue(msg.propertyExists("JMSXDeliveryCount"));
    }

    @Test
    public void testPropertyExistsWithInvalidName() throws JMSException {
        doPropertyExistsWithInvalidNameTestImpl(false);
    }

    private void doPropertyExistsWithInvalidNameTestImpl(boolean disableValidation) throws JMSException {
        String invalidPropName = "my-invalid-property";
        String valueA = "valueA";

        JmsMessage msg = factory.createMessage();
        if(disableValidation) {
            msg.setValidatePropertyNames(false);
        }

        assertTrue(msg.getFacade() instanceof JmsTestMessageFacade);
        JmsTestMessageFacade testFacade = (JmsTestMessageFacade) msg.getFacade();

        assertFalse(testFacade.propertyExists(invalidPropName));
        testFacade.setProperty(invalidPropName, valueA);
        assertTrue(testFacade.propertyExists(invalidPropName));

        if(!disableValidation) {
            assertFalse("Property should be indicated to not exist", msg.propertyExists(invalidPropName));
        } else {
            assertTrue("Property should be indicated to exist", msg.propertyExists(invalidPropName));
        }
    }

    @Test
    public void testGetBooleanProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "booleanProperty";
        msg.setBooleanProperty(name, true);
        assertTrue(msg.getBooleanProperty(name));
    }

    @Test
    public void testGetByteProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "byteProperty";
        msg.setByteProperty(name, (byte) 1);
        assertTrue(msg.getByteProperty(name) == 1);
    }

    @Test
    public void testGetShortProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "shortProperty";
        msg.setShortProperty(name, (short) 1);
        assertTrue(msg.getShortProperty(name) == 1);
    }

    @Test
    public void testGetIntProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "intProperty";
        msg.setIntProperty(name, 1);
        assertTrue(msg.getIntProperty(name) == 1);
    }

    @Test
    public void testGetLongProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "longProperty";
        msg.setLongProperty(name, 1);
        assertTrue(msg.getLongProperty(name) == 1);
    }

    @Test
    public void testGetFloatProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "floatProperty";
        msg.setFloatProperty(name, 1.3f);
        assertTrue(msg.getFloatProperty(name) == 1.3f);
    }

    @Test
    public void testGetDoubleProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "doubleProperty";
        msg.setDoubleProperty(name, 1.3d);
        assertTrue(msg.getDoubleProperty(name) == 1.3);
    }

    @Test
    public void testGetStringProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "stringProperty";
        msg.setStringProperty(name, name);
        assertTrue(msg.getStringProperty(name).equals(name));
    }

    @Test
    public void testGetObjectProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "floatProperty";
        msg.setFloatProperty(name, 1.3f);
        assertTrue(msg.getObjectProperty(name) instanceof Float);
        assertTrue(((Float) msg.getObjectProperty(name)).floatValue() == 1.3f);
    }

    @Test
    public void testGetObjectPropertyWithInvalidNameThrowsIAEByDefault() throws JMSException {
        doGetObjectPropertyNameValidationTestImpl(false);
    }

    private void doGetObjectPropertyNameValidationTestImpl(boolean disableValidation) throws JMSException {
        String invalidPropName1 = "my-invalid-property";
        String valueA = "valueA";

        JmsMessage msg = factory.createMessage();
        if(disableValidation) {
            msg.setValidatePropertyNames(false);
        }

        assertTrue(msg.getFacade() instanceof JmsTestMessageFacade);
        JmsTestMessageFacade testFacade = (JmsTestMessageFacade) msg.getFacade();

        assertNull(testFacade.getProperty(invalidPropName1));
        testFacade.setProperty(invalidPropName1, valueA);
        assertEquals(valueA, testFacade.getProperty(invalidPropName1));

        if (!disableValidation) {
            try {
                msg.getObjectProperty(invalidPropName1);
                fail("expected rejection of identifier");
            } catch (IllegalArgumentException iae) {
                //expected
            }
        } else {
            assertEquals(valueA, msg.getObjectProperty(invalidPropName1));
        }
    }

    @Test
    public void testGetPropertyNames() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propName = "floatProperty";
        msg.setFloatProperty(propName, 1.3f);
        String jmsxName = "JMSXDeliveryCount";
        msg.setIntProperty(jmsxName, 1);
        String headerName = "JMSRedelivered";
        msg.setBooleanProperty(headerName, false);
        boolean propNameFound = false;
        boolean jmsxNameFound = false;
        boolean headerNameFound1 = false;
        for (Enumeration<?> iter = msg.getPropertyNames(); iter.hasMoreElements();) {
            Object element = iter.nextElement();
            propNameFound |= element.equals(propName);
            jmsxNameFound |= element.equals(jmsxName);
            headerNameFound1 |= element.equals(headerName);
        }
        assertTrue("prop name not found", propNameFound);
        assertTrue("jmsx prop name not found", jmsxNameFound);
        // spec compliance, only non-'JMS header' props returned
        assertFalse("header name should not have been found", headerNameFound1);
    }

    @Test
    public void testGetPropertyNamesReturnsValidNamesByDefault() throws JMSException {
        doGetPropertyNamesResultFilteringTestImpl(false);
    }

    private void doGetPropertyNamesResultFilteringTestImpl(boolean disableValidation) throws JMSException {
        String invalidPropName1 = "my-invalid-property1";
        String invalidPropName2 = "my.invalid.property2";
        String validPropName = "my_valid_property";

        String valueA = "valueA";
        String valueB = "valueB";
        String valueC = "valueC";

        JmsMessage msg = factory.createMessage();
        if(disableValidation) {
            msg.setValidatePropertyNames(false);
        }

        assertTrue(msg.getFacade() instanceof JmsTestMessageFacade);
        JmsTestMessageFacade testFacade = (JmsTestMessageFacade) msg.getFacade();

        assertNull(testFacade.getProperty(invalidPropName1));
        assertNull(testFacade.getProperty(invalidPropName2));
        assertNull(testFacade.getProperty(validPropName));

        testFacade.setProperty(invalidPropName1, valueA);
        testFacade.setProperty(invalidPropName2, valueB);
        testFacade.setProperty(validPropName, valueC);

        assertEquals(valueA, testFacade.getProperty(invalidPropName1));
        assertEquals(valueB, testFacade.getProperty(invalidPropName2));
        assertEquals(valueC, testFacade.getProperty(validPropName));

        boolean invalidPropName1Found = false;
        boolean invalidPropName2Found = false;
        boolean validPropNameFound = false;
        for (Enumeration<?> iter = msg.getPropertyNames(); iter.hasMoreElements();) {
            Object element = iter.nextElement();
            invalidPropName1Found |= element.equals(invalidPropName1);
            invalidPropName2Found |= element.equals(invalidPropName2);
            validPropNameFound |= element.equals(validPropName);
        }

        if (!disableValidation) {
            assertFalse("Invalid prop name 1 was found", invalidPropName1Found);
            assertFalse("Invalid prop name 2 was found", invalidPropName2Found);
        } else {
            assertTrue("Invalid prop name 1 was not found", invalidPropName1Found);
            assertTrue("Invalid prop name 2 was not found", invalidPropName2Found);
        }
        assertTrue("valid prop name was not found", validPropNameFound);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testGetAllPropertyNames() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name1 = "floatProperty";
        msg.setFloatProperty(name1, 1.3f);
        String name2 = "JMSXDeliveryCount";
        msg.setIntProperty(name2, 1);
        String name3 = "JMSRedelivered";
        msg.setBooleanProperty(name3, false);
        boolean found1 = false;
        boolean found2 = false;
        boolean found3 = false;
        for (Enumeration iter = msg.getAllPropertyNames(); iter.hasMoreElements();) {
            Object element = iter.nextElement();
            found1 |= element.equals(name1);
            found2 |= element.equals(name2);
            found3 |= element.equals(name3);
        }
        assertTrue("prop name1 found", found1);
        assertTrue("prop name2 found", found2);
        assertTrue("prop name4 found", found3);
    }

    @Test
    public void testSetObjectProperty() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String name = "property";

        try {
            msg.setObjectProperty(name, "string");
            msg.setObjectProperty(name, Byte.valueOf("1"));
            msg.setObjectProperty(name, Short.valueOf("1"));
            msg.setObjectProperty(name, Integer.valueOf("1"));
            msg.setObjectProperty(name, Long.valueOf("1"));
            msg.setObjectProperty(name, Float.valueOf("1.1f"));
            msg.setObjectProperty(name, Double.valueOf("1.1"));
            msg.setObjectProperty(name, Boolean.TRUE);
            msg.setObjectProperty(name, null);
        } catch (MessageFormatException e) {
            fail("should accept object primitives and String");
        }
        try {
            msg.setObjectProperty(name, new byte[5]);
            fail("should accept only object primitives and String");
        } catch (MessageFormatException e) {
        }
        try {
            msg.setObjectProperty(name, new Object());
            fail("should accept only object primitives and String");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testConvertProperties() throws Exception {

        JmsMessage msg = factory.createMessage();

        msg.setStringProperty("stringProperty", "string");
        msg.setByteProperty("byteProperty", Byte.valueOf("1"));
        msg.setShortProperty("shortProperty", Short.valueOf("1"));
        msg.setIntProperty("intProperty", Integer.valueOf("1"));
        msg.setLongProperty("longProperty", Long.valueOf("1"));
        msg.setFloatProperty("floatProperty", Float.valueOf("1.1f"));
        msg.setDoubleProperty("doubleProperty", Double.valueOf("1.1"));
        msg.setBooleanProperty("booleanProperty", Boolean.TRUE);
        msg.setObjectProperty("nullProperty", null);

        assertEquals(msg.getFacade().getProperty("stringProperty"), "string");
        assertEquals(((Byte) msg.getFacade().getProperty("byteProperty")).byteValue(), 1);
        assertEquals(((Short) msg.getFacade().getProperty("shortProperty")).shortValue(), 1);
        assertEquals(((Integer) msg.getFacade().getProperty("intProperty")).intValue(), 1);
        assertEquals(((Long) msg.getFacade().getProperty("longProperty")).longValue(), 1);
        assertEquals(((Float) msg.getFacade().getProperty("floatProperty")).floatValue(), 1.1f, 0);
        assertEquals(((Double) msg.getFacade().getProperty("doubleProperty")).doubleValue(), 1.1, 0);
        assertEquals(((Boolean) msg.getFacade().getProperty("booleanProperty")).booleanValue(), true);
        assertNull(msg.getFacade().getProperty("nullProperty"));
    }

    @Test
    public void testSetNullProperty() throws JMSException {
        Message msg = factory.createMessage();
        String name = "cheese";
        msg.setStringProperty(name, "Cheddar");
        assertEquals("Cheddar", msg.getStringProperty(name));

        msg.setStringProperty(name, null);
        assertEquals(null, msg.getStringProperty(name));
    }

    @Test
    public void testSetNullPropertyName() throws JMSException {
        JmsMessage msg = factory.createMessage();

        try {
            msg.setStringProperty(null, "Cheese");
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            LOG.info("Worked, caught: " + e);
        }
    }

    @Test
    public void testSetEmptyPropertyName() throws JMSException {
        JmsMessage msg = factory.createMessage();

        try {
            msg.setStringProperty("", "Cheese");
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            LOG.info("Worked, caught: " + e);
        }
    }

    @Test
    public void testGetAndSetJMSXDeliveryCount() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.setIntProperty("JMSXDeliveryCount", 1);
        int count = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("expected delivery count = 1 - got: " + count, count == 1);
    }

    @Test
    public void testClearBody() throws JMSException {
        JmsBytesMessage message = factory.createBytesMessage();
        message.clearBody();
        assertFalse(message.isReadOnlyBody());
        message.reset();
        assertEquals(0, message.getBodyLength());
    }

    @Test
    public void testBooleanPropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        msg.setBooleanProperty(propertyName, true);

        assertEquals(((Boolean) msg.getObjectProperty(propertyName)).booleanValue(), true);
        assertTrue(msg.getBooleanProperty(propertyName));
        assertEquals(msg.getStringProperty(propertyName), "true");
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testBytePropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        msg.setByteProperty(propertyName, (byte) 1);

        assertEquals(((Byte) msg.getObjectProperty(propertyName)).byteValue(), 1);
        assertEquals(msg.getByteProperty(propertyName), 1);
        assertEquals(msg.getShortProperty(propertyName), 1);
        assertEquals(msg.getIntProperty(propertyName), 1);
        assertEquals(msg.getLongProperty(propertyName), 1);
        assertEquals(msg.getStringProperty(propertyName), "1");
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testShortPropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        msg.setShortProperty(propertyName, (short) 1);

        assertEquals(((Short) msg.getObjectProperty(propertyName)).shortValue(), 1);
        assertEquals(msg.getShortProperty(propertyName), 1);
        assertEquals(msg.getIntProperty(propertyName), 1);
        assertEquals(msg.getLongProperty(propertyName), 1);
        assertEquals(msg.getStringProperty(propertyName), "1");
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testIntPropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        msg.setIntProperty(propertyName, 1);

        assertEquals(((Integer) msg.getObjectProperty(propertyName)).intValue(), 1);
        assertEquals(msg.getIntProperty(propertyName), 1);
        assertEquals(msg.getLongProperty(propertyName), 1);
        assertEquals(msg.getStringProperty(propertyName), "1");
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testLongPropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        msg.setLongProperty(propertyName, 1);

        assertEquals(((Long) msg.getObjectProperty(propertyName)).longValue(), 1);
        assertEquals(msg.getLongProperty(propertyName), 1);
        assertEquals(msg.getStringProperty(propertyName), "1");
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testFloatPropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        msg.setFloatProperty(propertyName, (float) 1.5);
        assertEquals(((Float) msg.getObjectProperty(propertyName)).floatValue(), 1.5, 0);
        assertEquals(msg.getFloatProperty(propertyName), 1.5, 0);
        assertEquals(msg.getDoubleProperty(propertyName), 1.5, 0);
        assertEquals(msg.getStringProperty(propertyName), "1.5");
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testDoublePropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        msg.setDoubleProperty(propertyName, 1.5);
        assertEquals(((Double) msg.getObjectProperty(propertyName)).doubleValue(), 1.5, 0);
        assertEquals(msg.getDoubleProperty(propertyName), 1.5, 0);
        assertEquals(msg.getStringProperty(propertyName), "1.5");
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testStringPropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        String stringValue = "true";
        msg.setStringProperty(propertyName, stringValue);
        assertEquals(msg.getStringProperty(propertyName), stringValue);
        assertEquals(msg.getObjectProperty(propertyName), stringValue);
        assertEquals(msg.getBooleanProperty(propertyName), true);

        stringValue = "1";
        msg.setStringProperty(propertyName, stringValue);
        assertEquals(msg.getByteProperty(propertyName), 1);
        assertEquals(msg.getShortProperty(propertyName), 1);
        assertEquals(msg.getIntProperty(propertyName), 1);
        assertEquals(msg.getLongProperty(propertyName), 1);

        stringValue = "1.5";
        msg.setStringProperty(propertyName, stringValue);
        assertEquals(msg.getFloatProperty(propertyName), 1.5, 0);
        assertEquals(msg.getDoubleProperty(propertyName), 1.5, 0);

        stringValue = "bad";
        msg.setStringProperty(propertyName, stringValue);
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (NumberFormatException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (NumberFormatException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (NumberFormatException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (NumberFormatException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (NumberFormatException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (NumberFormatException e) {
        }
        assertFalse(msg.getBooleanProperty(propertyName));
    }

    @Test
    public void testObjectPropertyConversion() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        Object obj = new Object();
        msg.getFacade().setProperty(propertyName, obj);
        try {
            msg.getObjectProperty(null);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
        }
        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }

    }

    @Test
    public void testReadOnlyProperties() throws JMSException {
        JmsMessage msg = factory.createMessage();
        String propertyName = "property";
        msg.setReadOnlyProperties(true);

        try {
            msg.setObjectProperty(propertyName, new Object());
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
        try {
            msg.setStringProperty(propertyName, "test");
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
        try {
            msg.setBooleanProperty(propertyName, true);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
        try {
            msg.setByteProperty(propertyName, (byte) 1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
        try {
            msg.setShortProperty(propertyName, (short) 1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
        try {
            msg.setIntProperty(propertyName, 1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
        try {
            msg.setLongProperty(propertyName, 1);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
        try {
            msg.setFloatProperty(propertyName, (float) 1.5);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
        try {
            msg.setDoubleProperty(propertyName, 1.5);
            fail("Should have thrown exception");
        } catch (MessageNotWriteableException e) {
        }
    }

    @Test
    public void testAcknowledgeWithNoCallbackDoesNotThrow() throws JMSException {
        JmsMessage msg = factory.createMessage();
        msg.acknowledge();
    }

    //---- Test that message property getters throw expected exceptions ------//

    /**
     * When a property is not set, the behaviour of JMS specifies that it is equivalent to a null value,
     * and the primitive property accessors should behave in the same fashion as <primitive>.valueOf(String).
     * Test that this is the case.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testGetMissingPrimitivePropertyResultsInExpectedBehaviour() throws Exception {
        String propertyName = "does_not_exist";
        JmsMessage testMessage = factory.createMessage();

        //expect false from Boolean.valueOf(null).
        assertFalse(testMessage.getBooleanProperty(propertyName));

        //expect an NFE from the primitive integral <type>.valueOf(null) conversions
        assertGetMissingPropertyThrowsNumberFormatException(testMessage, propertyName, Byte.class);
        assertGetMissingPropertyThrowsNumberFormatException(testMessage, propertyName, Short.class);
        assertGetMissingPropertyThrowsNumberFormatException(testMessage, propertyName, Integer.class);
        assertGetMissingPropertyThrowsNumberFormatException(testMessage, propertyName, Long.class);

        // expect an NPE from the primitive floating point .valueOf(null)
        // conversions
        try {
            testMessage.getFloatProperty(propertyName);
            fail("expected NPE from Float.valueOf(null) was not thrown");
        } catch (NullPointerException npe) {
        }

        try {
            testMessage.getDoubleProperty(propertyName);
            fail("expected NPE from Double.valueOf(null) was not thrown");
        } catch (NullPointerException npe) {
        }
    }

    //---------- Test Message Properties enforce compliant names -------------//

    /**
     * Property 'identifiers' (i.e. names) must begin with a letter for which
     * {@link Character#isJavaLetter(char)} is true, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * beginning with a non-letter character.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNonLetterAsFirstCharacterThrowsIAE() throws Exception {
        String propertyName = "1name";
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty(propertyName, "value");
            fail("expected rejection of identifier starting with non-letter character");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) must continue with a letter or digit
     * for which {@link Character#isJavaLetterOrDigit(char)} is true, as
     * described in {@link javax.jms.Message}. Verify an IAE is thrown if
     * setting a property continuing with a non-letter-or-digit character.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNonLetterOrDigitCharacterThrowsIAE() throws Exception {
        doSetObjectPropertyWithNonLetterOrDigitCharacterTestImpl(false);
    }

    private void doSetObjectPropertyWithNonLetterOrDigitCharacterTestImpl(boolean disableValidation) throws JMSException {
        String invalidPropertyName = "name-invalid";
        String valueA = "value";

        JmsMessage msg = factory.createMessage();
        if(disableValidation) {
            msg.setValidatePropertyNames(false);
        }

        assertTrue(msg.getFacade() instanceof JmsTestMessageFacade);
        JmsTestMessageFacade testFacade = (JmsTestMessageFacade) msg.getFacade();

        if (!disableValidation) {
            try {
                msg.setObjectProperty(invalidPropertyName, valueA);
                fail("expected rejection of identifier starting with non-letter character");
            } catch (IllegalArgumentException iae) {
                // Expected
            }
        } else {
            msg.setObjectProperty(invalidPropertyName, valueA);

            assertEquals(valueA, testFacade.getProperty(invalidPropertyName));
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NULL, TRUE, or
     * FALSE, as described in {@link javax.jms.Message}. Verify an IAE is thrown
     * if setting a property with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameNULL() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("NULL", "value");
            fail("expected rejection of identifier named NULL");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NULL, TRUE, or
     * FALSE, as described in {@link javax.jms.Message}. Verify an IAE is thrown
     * if setting a property with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameTRUE() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("TRUE", "value");
            fail("expected rejection of identifier named TRUE");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NULL, TRUE, or
     * FALSE, as described in {@link javax.jms.Message}. Verify an IAE is thrown
     * if setting a property with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameFALSE() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("FALSE", "value");
            fail("expected rejection of identifier named FALSE");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NOT, AND, OR,
     * BETWEEN, LIKE, IN, IS, or ESCAPE, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameNOT() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("NOT", "value");
            fail("expected rejection of identifier named NOT");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NOT, AND, OR,
     * BETWEEN, LIKE, IN, IS, or ESCAPE, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameAND() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("AND", "value");
            fail("expected rejection of identifier named AND");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NOT, AND, OR,
     * BETWEEN, LIKE, IN, IS, or ESCAPE, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameOR() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("OR", "value");
            fail("expected rejection of identifier named OR");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NOT, AND, OR,
     * BETWEEN, LIKE, IN, IS, or ESCAPE, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameBETWEEN() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("BETWEEN", "value");
            fail("expected rejection of identifier named BETWEEN");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NOT, AND, OR,
     * BETWEEN, LIKE, IN, IS, or ESCAPE, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameLIKE() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("LIKE", "value");
            fail("expected rejection of identifier named LIKE");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NOT, AND, OR,
     * BETWEEN, LIKE, IN, IS, or ESCAPE, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameIN() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("IN", "value");
            fail("expected rejection of identifier named IN");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NOT, AND, OR,
     * BETWEEN, LIKE, IN, IS, or ESCAPE, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameIS() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("IS", "value");
            fail("expected rejection of identifier named IS");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Property 'identifiers' (i.e. names) are not allowed to be NOT, AND, OR,
     * BETWEEN, LIKE, IN, IS, or ESCAPE, as described in
     * {@link javax.jms.Message}. Verify an IAE is thrown if setting a property
     * with these values.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyWithNameESCAPE() throws Exception {
        JmsMessage testMessage = factory.createMessage();
        try {
            testMessage.setObjectProperty("ESCAPE", "value");
            fail("expected rejection of identifier named ESCAPE");
        } catch (IllegalArgumentException iae) {
        }
    }

    //---------- Test disabling Message Properties name validation -------------//

    @Test
    public void testGetPropertyNamesReturnsAllNamesWithValidationDisabled() throws JMSException {
        doGetPropertyNamesResultFilteringTestImpl(true);
    }

    @Test
    public void testGetObjectPropertyWithInvalidNameAndValidationDisabled() throws JMSException {
        doGetObjectPropertyNameValidationTestImpl(true);
    }

    @Test
    public void testSetObjectPropertyWithInvalidNameAndValidationDisabled() throws Exception {
        doSetObjectPropertyWithNonLetterOrDigitCharacterTestImpl(true);
    }

    @Test
    public void testPropertyExistsWithInvalidNameAndValidationDisabled() throws JMSException {
        doPropertyExistsWithInvalidNameTestImpl(true);
    }

    //---------- Test ack type modifier property -------------//

    /**
     * Basic test that the JMS_AMQP_ACK_TYPE property is intercepted and has
     * effect on messages with an acknowledgement callback. More detailed tests
     * of usage and effect of this property is performed elsewhere.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSetPropertyJMS_AMQP_ACK_TYPE() throws Exception {
        JmsMessage message = factory.createMessage();
        JmsSession session = Mockito.mock(JmsSession.class);
        JmsAcknowledgeCallback callback = new JmsAcknowledgeCallback(session);
        message.setAcknowledgeCallback(callback);

        assertEquals("Unexpected ack type value", ACCEPTED, callback.getAckType());

        message.setIntProperty(JMS_AMQP_ACK_TYPE, RELEASED);

        assertEquals("Unexpected ack type value after setting prop", RELEASED, callback.getAckType());
    }

    //--------- Test isBodyAssignableTo method -------------------------------//

    @Test
    public void testMessageIsBodyAssignableTo() throws Exception {
        Message message = factory.createMessage();

        assertTrue(message.isBodyAssignableTo(String.class));
        assertTrue(message.isBodyAssignableTo(Boolean.class));
        assertTrue(message.isBodyAssignableTo(Object.class));
        assertTrue(message.isBodyAssignableTo(Map.class));
    }

    @Test
    public void testTextMessageIsBodyAssignableTo() throws Exception {
        JmsTextMessage message = factory.createTextMessage();

        assertTrue(message.isBodyAssignableTo(String.class));
        assertTrue(message.isBodyAssignableTo(Boolean.class));
        assertTrue(message.isBodyAssignableTo(Map.class));
        assertTrue(message.isBodyAssignableTo(Object.class));

        message.setText("test");

        assertTrue(message.isBodyAssignableTo(String.class));
        assertFalse(message.isBodyAssignableTo(Boolean.class));
        assertFalse(message.isBodyAssignableTo(Map.class));
        assertTrue(message.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testStreamMessageIsBodyAssignableTo() throws Exception {
        JmsStreamMessage message = factory.createStreamMessage();

        assertFalse(message.isBodyAssignableTo(String.class));
        assertFalse(message.isBodyAssignableTo(Boolean.class));
        assertFalse(message.isBodyAssignableTo(Map.class));
        assertFalse(message.isBodyAssignableTo(Object.class));

        message.writeBoolean(false);

        assertFalse(message.isBodyAssignableTo(String.class));
        assertFalse(message.isBodyAssignableTo(Boolean.class));
        assertFalse(message.isBodyAssignableTo(Map.class));
        assertFalse(message.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testMapMessageIsBodyAssignableTo() throws Exception {
        JmsMapMessage message = factory.createMapMessage();

        assertTrue(message.isBodyAssignableTo(String.class));
        assertTrue(message.isBodyAssignableTo(Boolean.class));
        assertTrue(message.isBodyAssignableTo(Map.class));
        assertTrue(message.isBodyAssignableTo(Object.class));

        message.setBoolean("Boolean", true);

        assertFalse(message.isBodyAssignableTo(String.class));
        assertFalse(message.isBodyAssignableTo(Boolean.class));
        assertTrue(message.isBodyAssignableTo(Map.class));
        assertTrue(message.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testBytesMessageIsBodyAssignableTo() throws Exception {
        JmsBytesMessage message = factory.createBytesMessage();

        assertTrue(message.isBodyAssignableTo(byte[].class));
        assertTrue(message.isBodyAssignableTo(Boolean.class));
        assertTrue(message.isBodyAssignableTo(Map.class));
        assertTrue(message.isBodyAssignableTo(String.class));
        assertTrue(message.isBodyAssignableTo(Object.class));

        message.writeBoolean(false);

        // The message doesn't technically have a body until it is reset
        message.reset();

        assertTrue(message.isBodyAssignableTo(byte[].class));
        assertFalse(message.isBodyAssignableTo(Boolean.class));
        assertFalse(message.isBodyAssignableTo(Map.class));
        assertFalse(message.isBodyAssignableTo(String.class));
        assertTrue(message.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testObjectMessageIsBodyAssignableTo() throws Exception {
        JmsObjectMessage message = factory.createObjectMessage();

        assertTrue(message.isBodyAssignableTo(Boolean.class));
        assertTrue(message.isBodyAssignableTo(Map.class));
        assertTrue(message.isBodyAssignableTo(String.class));
        assertTrue(message.isBodyAssignableTo(Serializable.class));
        assertTrue(message.isBodyAssignableTo(Object.class));

        message.setObject(UUID.randomUUID());

        assertFalse(message.isBodyAssignableTo(Boolean.class));
        assertFalse(message.isBodyAssignableTo(Map.class));
        assertFalse(message.isBodyAssignableTo(String.class));
        assertTrue(message.isBodyAssignableTo(Serializable.class));
        assertTrue(message.isBodyAssignableTo(Object.class));
        assertTrue(message.isBodyAssignableTo(UUID.class));
    }

    //--------- Test for getBody method --------------------------------------//

    @Test
    public void testGetBodyOnMessage() throws Exception {
        Message message = factory.createMessage();

        assertNull(message.getBody(String.class));
        assertNull(message.getBody(Boolean.class));
        assertNull(message.getBody(byte[].class));
        assertNull(message.getBody(Object.class));
    }

    @Test
    public void testGetBodyOnTextMessage() throws Exception {
        TextMessage message = factory.createTextMessage();

        assertNull(message.getBody(String.class));
        assertNull(message.getBody(Boolean.class));
        assertNull(message.getBody(byte[].class));
        assertNull(message.getBody(Object.class));

        message.setText("test");

        assertNotNull(message.getBody(String.class));
        assertNotNull(message.getBody(Object.class));

        try {
            message.getBody(Boolean.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(Map.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(byte[].class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }
    }

    @Test
    public void testGetBodyOnMapMessage() throws Exception {
        MapMessage message = factory.createMapMessage();

        assertNull(message.getBody(String.class));
        assertNull(message.getBody(Boolean.class));
        assertNull(message.getBody(byte[].class));
        assertNull(message.getBody(Object.class));

        message.setString("test", "test");

        assertNotNull(message.getBody(Map.class));
        assertNotNull(message.getBody(Object.class));

        try {
            message.getBody(Boolean.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(String.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(byte[].class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }
    }

    @Test
    public void testGetBodyOnObjectMessage() throws Exception {
        ObjectMessage message = factory.createObjectMessage();

        assertNull(message.getBody(String.class));
        assertNull(message.getBody(Boolean.class));
        assertNull(message.getBody(byte[].class));
        assertNull(message.getBody(Serializable.class));
        assertNull(message.getBody(Object.class));

        message.setObject(UUID.randomUUID());

        assertNotNull(message.getBody(UUID.class));
        assertNotNull(message.getBody(Serializable.class));
        assertNotNull(message.getBody(Object.class));

        try {
            message.getBody(Boolean.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(String.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(byte[].class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }
    }

    @Test
    public void testGetBodyOnBytesMessage() throws Exception {
        BytesMessage message = factory.createBytesMessage();

        assertNull(message.getBody(String.class));
        assertNull(message.getBody(Boolean.class));
        assertNull(message.getBody(byte[].class));
        assertNull(message.getBody(Object.class));

        message.clearBody();

        message.writeUTF("test");
        message.reset();

        assertNotNull(message.getBody(byte[].class));
        assertNotNull(message.getBody(Object.class));

        try {
            message.getBody(Boolean.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(Map.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(String.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }
    }

    @Test
    public void testGetBodyOnStreamMessage() throws Exception {
        StreamMessage message = factory.createStreamMessage();

        try {
            message.getBody(Object.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(Boolean.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(String.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(byte[].class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        message.writeBoolean(false);

        try {
            message.getBody(Object.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(Boolean.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(String.class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }

        try {
            message.getBody(byte[].class);
            fail("Should have thrown an exception");
        } catch (MessageFormatException mfe) {
            LOG.info("caught expected MessageFormatException");
        }
    }

    //--------- Test support method ------------------------------------------//

    private void assertGetMissingPropertyThrowsNumberFormatException(JmsMessage testMessage, String propertyName, Class<?> clazz) throws JMSException {
        try {
            getMessagePropertyUsingTypeMethod(testMessage, propertyName, clazz);
            fail("expected exception to be thrown");
        } catch (NumberFormatException nfe) {
        }
    }

    private Object getMessagePropertyUsingTypeMethod(JmsMessage testMessage, String propertyName, Class<?> clazz) throws JMSException {
        if (clazz == Boolean.class) {
            return testMessage.getBooleanProperty(propertyName);
        } else if (clazz == Byte.class) {
            return testMessage.getByteProperty(propertyName);
        } else if (clazz == Short.class) {
            return testMessage.getShortProperty(propertyName);
        } else if (clazz == Integer.class) {
            return testMessage.getIntProperty(propertyName);
        } else if (clazz == Long.class) {
            return testMessage.getLongProperty(propertyName);
        } else if (clazz == Float.class) {
            return testMessage.getFloatProperty(propertyName);
        } else if (clazz == Double.class) {
            return testMessage.getDoubleProperty(propertyName);
        } else if (clazz == String.class) {
            return testMessage.getStringProperty(propertyName);
        } else {
            throw new RuntimeException("Unexpected property type class");
        }
    }
}
