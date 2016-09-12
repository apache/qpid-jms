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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import javax.jms.JMSProducer;
import javax.jms.MessageFormatRuntimeException;

import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsContext;
import org.junit.Before;
import org.junit.Test;

/**
 * Test various behaviors of the JMSProducer implementation.
 */
public class JmsProducerTest extends JmsConnectionTestSupport {

    private JmsContext context;

    private final String BAD_PROPERTY_NAME = "%_BAD_PROPERTY_NAME";
    private final String GOOD_PROPERTY_NAME = "GOOD_PROPERTY_NAME";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        context = createJMSContextToMockProvider();
    }

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
}
