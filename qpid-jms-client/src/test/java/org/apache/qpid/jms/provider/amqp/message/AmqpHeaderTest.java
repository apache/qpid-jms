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
package org.apache.qpid.jms.provider.amqp.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.junit.Test;

/**
 * Test the AmqpHeader implementation
 */
public class AmqpHeaderTest {

    @Test
    public void testCreate() {
        AmqpHeader header = new AmqpHeader();

        assertTrue(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertNotNull(header.toString());
    }

    @Test
    public void testCreateFromHeader() {
        Header protonHeader = new Header();
        protonHeader.setPriority(UnsignedByte.valueOf((byte) 9));
        protonHeader.setTtl(UnsignedInteger.valueOf(10));
        protonHeader.setDeliveryCount(UnsignedInteger.valueOf(11));
        protonHeader.setDurable(true);
        protonHeader.setFirstAcquirer(true);

        AmqpHeader header = new AmqpHeader(protonHeader);

        assertFalse(header.isDefault());
        assertTrue(header.nonDefaultDurable());
        assertTrue(header.nonDefaultPriority());
        assertTrue(header.nonDefaultTimeToLive());
        assertTrue(header.nonDefaultFirstAcquirer());
        assertTrue(header.nonDefaultDeliveryCount());

        assertEquals(true, header.isDurable());
        assertEquals(true, header.isFirstAcquirer());
        assertEquals(9, header.getPriority());
        assertEquals(10, header.getTimeToLive());
        assertEquals(11, header.getDeliveryCount());
    }

    @Test
    public void testCreateFromAmqpHeader() {
        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setPriority(UnsignedByte.valueOf((byte) 9));
        amqpHeader.setTimeToLive(UnsignedInteger.valueOf(10));
        amqpHeader.setDeliveryCount(UnsignedInteger.valueOf(11));
        amqpHeader.setDurable(true);
        amqpHeader.setFirstAcquirer(true);

        AmqpHeader header = new AmqpHeader(amqpHeader);

        assertFalse(header.isDefault());
        assertTrue(header.nonDefaultDurable());
        assertTrue(header.nonDefaultPriority());
        assertTrue(header.nonDefaultTimeToLive());
        assertTrue(header.nonDefaultFirstAcquirer());
        assertTrue(header.nonDefaultDeliveryCount());

        assertEquals(true, header.isDurable());
        assertEquals(true, header.isFirstAcquirer());
        assertEquals(9, header.getPriority());
        assertEquals(10, header.getTimeToLive());
        assertEquals(11, header.getDeliveryCount());
    }

    //----- Test getHeader default processing-----------------------------//

    @Test
    public void testGetHeaderReturnsNullWhenAllDefault() {
        AmqpHeader header = new AmqpHeader();

        assertNull("Header should not have been created as values are defaulted", header.getHeader());
    }

    @Test
    public void testGetHeaderWhenDurableIsNotDefault() {
        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setDurable(true);

        Header header = amqpHeader.getHeader();
        assertNotNull("Header should have been created as values are not all defaulted", header);
        assertTrue(header.getDurable());
    }

    @Test
    public void testGetHeaderWhenDeliveryCountIsNotDefault() {
        AmqpHeader amqpHeader = new AmqpHeader();
        int count = 234;
        amqpHeader.setDeliveryCount(count);

        Header header = amqpHeader.getHeader();
        assertNotNull("Header should have been created as values are not all defaulted", header);
        assertEquals(UnsignedInteger.valueOf(count), header.getDeliveryCount());
    }

    @Test
    public void testGetHeaderWhenPriorityIsNotDefault() {
        AmqpHeader amqpHeader = new AmqpHeader();
        byte priority = 6;
        amqpHeader.setPriority(priority);

        Header header = amqpHeader.getHeader();
        assertNotNull("Header should have been created as values are not all defaulted", header);
        assertEquals(UnsignedByte.valueOf(priority), header.getPriority());
    }

    @Test
    public void testGetHeaderWhenTTLIsNotDefault() {
        AmqpHeader amqpHeader = new AmqpHeader();
        int ttl = 345;
        amqpHeader.setTimeToLive(ttl);

        Header header = amqpHeader.getHeader();
        assertNotNull("Header should have been created as values are not all defaulted", header);
        assertEquals(UnsignedInteger.valueOf(ttl), header.getTtl());
    }

    @Test
    public void testGetHeaderWhenFirstAcquirerIsNotDefault() {
        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setFirstAcquirer(true);

        Header header = amqpHeader.getHeader();
        assertNotNull("Header should have been created as values are not all defaulted", header);
        assertTrue(header.getFirstAcquirer());
    }

    //----- Test Set from Header ---------------------------------------------//

    @Test
    public void testSetHeaderWithNull() {
        AmqpHeader header = new AmqpHeader();

        header.setHeader((Header) null);

        assertTrue(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithDefaultHeader() {
        AmqpHeader header = new AmqpHeader();

        header.setHeader(new Header());

        assertTrue(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithDurableHeader() {
        AmqpHeader header = new AmqpHeader();

        Header protonHeader = new Header();
        protonHeader.setDurable(true);

        header.setHeader(protonHeader);

        assertFalse(header.isDefault());
        assertTrue(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(true, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithFirstAcquirerHeader() {
        AmqpHeader header = new AmqpHeader();

        Header protonHeader = new Header();
        protonHeader.setFirstAcquirer(true);

        header.setHeader(protonHeader);

        assertFalse(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertTrue(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(true, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithPriorityHeader() {
        AmqpHeader header = new AmqpHeader();

        Header protonHeader = new Header();
        protonHeader.setPriority(UnsignedByte.valueOf((byte) 9));

        header.setHeader(protonHeader);

        assertFalse(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertTrue(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(9, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithDeliveryCountHeader() {
        AmqpHeader header = new AmqpHeader();

        Header protonHeader = new Header();
        protonHeader.setDeliveryCount(UnsignedInteger.valueOf(9));

        header.setHeader(protonHeader);

        assertFalse(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertTrue(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(9, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithTimeToLiveHeader() {
        AmqpHeader header = new AmqpHeader();

        Header protonHeader = new Header();
        protonHeader.setTtl(UnsignedInteger.valueOf(9));

        header.setHeader(protonHeader);

        assertFalse(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertTrue(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(9, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithHeaderWithAllSet() {
        AmqpHeader header = new AmqpHeader();

        Header protonHeader = new Header();
        protonHeader.setPriority(UnsignedByte.valueOf((byte) 9));
        protonHeader.setTtl(UnsignedInteger.valueOf(10));
        protonHeader.setDeliveryCount(UnsignedInteger.valueOf(11));
        protonHeader.setDurable(true);
        protonHeader.setFirstAcquirer(true);

        header.setHeader(protonHeader);

        assertFalse(header.isDefault());
        assertTrue(header.nonDefaultDurable());
        assertTrue(header.nonDefaultPriority());
        assertTrue(header.nonDefaultTimeToLive());
        assertTrue(header.nonDefaultFirstAcquirer());
        assertTrue(header.nonDefaultDeliveryCount());

        assertEquals(true, header.isDurable());
        assertEquals(true, header.isFirstAcquirer());
        assertEquals(9, header.getPriority());
        assertEquals(10, header.getTimeToLive());
        assertEquals(11, header.getDeliveryCount());
    }

    //----- Test Set from AmqpHeader ---------------------------------------------//

    @Test
    public void testSetAmqpHeaderWithNull() {
        AmqpHeader header = new AmqpHeader();

        header.setHeader((AmqpHeader) null);

        assertTrue(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithDefaultAmqpHeader() {
        AmqpHeader header = new AmqpHeader();

        header.setHeader(new AmqpHeader());

        assertTrue(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithDurableAmqpHeader() {
        AmqpHeader header = new AmqpHeader();

        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setDurable(true);

        header.setHeader(amqpHeader);

        assertFalse(header.isDefault());
        assertTrue(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(true, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithFirstAcquirerAmqpHeader() {
        AmqpHeader header = new AmqpHeader();

        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setFirstAcquirer(true);

        header.setHeader(amqpHeader);

        assertFalse(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertTrue(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(true, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithPriorityAmqpHeader() {
        AmqpHeader header = new AmqpHeader();

        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setPriority(UnsignedByte.valueOf((byte) 9));

        header.setHeader(amqpHeader);

        assertFalse(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertTrue(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(9, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithDeliveryCountAmqpHeader() {
        AmqpHeader header = new AmqpHeader();

        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setDeliveryCount(UnsignedInteger.valueOf(9));

        header.setHeader(amqpHeader);

        assertFalse(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertFalse(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertTrue(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(0, header.getTimeToLive());
        assertEquals(9, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithTimeToLiveAmqpHeader() {
        AmqpHeader header = new AmqpHeader();

        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setTimeToLive(UnsignedInteger.valueOf(9));

        header.setHeader(amqpHeader);

        assertFalse(header.isDefault());
        assertFalse(header.nonDefaultDurable());
        assertFalse(header.nonDefaultPriority());
        assertTrue(header.nonDefaultTimeToLive());
        assertFalse(header.nonDefaultFirstAcquirer());
        assertFalse(header.nonDefaultDeliveryCount());

        assertEquals(false, header.isDurable());
        assertEquals(false, header.isFirstAcquirer());
        assertEquals(4, header.getPriority());
        assertEquals(9, header.getTimeToLive());
        assertEquals(0, header.getDeliveryCount());
    }

    @Test
    public void testSetHeaderWithAmqpHeaderWithAllSet() {
        AmqpHeader header = new AmqpHeader();

        AmqpHeader amqpHeader = new AmqpHeader();
        amqpHeader.setPriority(UnsignedByte.valueOf((byte) 9));
        amqpHeader.setTimeToLive(UnsignedInteger.valueOf(10));
        amqpHeader.setDeliveryCount(UnsignedInteger.valueOf(11));
        amqpHeader.setDurable(true);
        amqpHeader.setFirstAcquirer(true);

        header.setHeader(amqpHeader);

        assertFalse(header.isDefault());
        assertTrue(header.nonDefaultDurable());
        assertTrue(header.nonDefaultPriority());
        assertTrue(header.nonDefaultTimeToLive());
        assertTrue(header.nonDefaultFirstAcquirer());
        assertTrue(header.nonDefaultDeliveryCount());

        assertEquals(true, header.isDurable());
        assertEquals(true, header.isFirstAcquirer());
        assertEquals(9, header.getPriority());
        assertEquals(10, header.getTimeToLive());
        assertEquals(11, header.getDeliveryCount());
    }

    //----- Test Durable Property --------------------------------------------//

    @Test
    public void testSetDurableFromNull() {
        AmqpHeader header = new AmqpHeader();

        header.setDurable((Boolean) null);

        assertFalse(header.isDurable());
        assertFalse(header.nonDefaultDurable());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetDurableFromBoolean() {
        AmqpHeader header = new AmqpHeader();

        header.setDurable(Boolean.FALSE);

        assertFalse(header.isDurable());
        assertFalse(header.nonDefaultDurable());

        assertTrue(header.isDefault());

        header.setDurable(Boolean.TRUE);

        assertTrue(header.isDurable());
        assertTrue(header.nonDefaultDurable());

        assertFalse(header.isDefault());
    }

    @Test
    public void testSetDurableFromPrimitive() {
        AmqpHeader header = new AmqpHeader();

        header.setDurable(false);

        assertFalse(header.isDurable());
        assertFalse(header.nonDefaultDurable());

        assertTrue(header.isDefault());

        header.setDurable(true);

        assertTrue(header.isDurable());
        assertTrue(header.nonDefaultDurable());

        assertFalse(header.isDefault());
    }

    //----- Test Priority Property -------------------------------------------//

    @Test
    public void testSetPriorityFromNull() {
        AmqpHeader header = new AmqpHeader();

        header.setPriority((UnsignedByte) null);

        assertEquals(4, header.getPriority());
        assertFalse(header.nonDefaultPriority());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetPriorityFromUnsigedByte() {
        AmqpHeader header = new AmqpHeader();

        header.setPriority(UnsignedByte.valueOf((byte) 9));

        assertEquals(9, header.getPriority());
        assertTrue(header.nonDefaultPriority());

        assertFalse(header.isDefault());

        header.setPriority(UnsignedByte.valueOf((byte) 4));

        assertEquals(4, header.getPriority());
        assertFalse(header.nonDefaultPriority());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetPriorityFromPrimitive() {
        AmqpHeader header = new AmqpHeader();

        header.setPriority(9);

        assertEquals(9, header.getPriority());
        assertTrue(header.nonDefaultPriority());

        assertFalse(header.isDefault());

        header.setPriority(4);

        assertEquals(4, header.getPriority());
        assertFalse(header.nonDefaultPriority());

        assertTrue(header.isDefault());
    }

    //----- Test Time To Live Property ---------------------------------------//

    @Test
    public void testSetTimeToLiveFromNull() {
        AmqpHeader header = new AmqpHeader();

        header.setTimeToLive((UnsignedInteger) null);

        assertEquals(0, header.getTimeToLive());
        assertFalse(header.nonDefaultTimeToLive());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetTimeToLiveFromLongMaxValue() {
        AmqpHeader header = new AmqpHeader();

        header.setTimeToLive(Long.MAX_VALUE);

        assertEquals(0, header.getTimeToLive());
        assertFalse(header.nonDefaultTimeToLive());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetTimeToLiveFromUnsigedByte() {
        AmqpHeader header = new AmqpHeader();

        header.setTimeToLive(UnsignedInteger.valueOf((byte) 90));

        assertEquals(90, header.getTimeToLive());
        assertTrue(header.nonDefaultTimeToLive());

        assertFalse(header.isDefault());

        header.setTimeToLive(UnsignedInteger.valueOf((byte) 0));

        assertEquals(0, header.getTimeToLive());
        assertFalse(header.nonDefaultTimeToLive());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetTimeToLiveFromPrimitive() {
        AmqpHeader header = new AmqpHeader();

        header.setTimeToLive(9);

        assertEquals(9, header.getTimeToLive());
        assertTrue(header.nonDefaultTimeToLive());

        assertFalse(header.isDefault());

        header.setTimeToLive(0);

        assertEquals(0, header.getTimeToLive());
        assertFalse(header.nonDefaultTimeToLive());

        assertTrue(header.isDefault());
    }

    //----- Test First Acquirer Property ---------------------------------------//

    @Test
    public void testSetFirstAcquirerFromNull() {
        AmqpHeader header = new AmqpHeader();

        header.setFirstAcquirer((Boolean) null);

        assertFalse(header.isFirstAcquirer());
        assertFalse(header.nonDefaultFirstAcquirer());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetFirstAcquirerFromBoolean() {
        AmqpHeader header = new AmqpHeader();

        header.setFirstAcquirer(Boolean.FALSE);

        assertFalse(header.isFirstAcquirer());
        assertFalse(header.nonDefaultFirstAcquirer());

        assertTrue(header.isDefault());

        header.setFirstAcquirer(Boolean.TRUE);

        assertTrue(header.isFirstAcquirer());
        assertTrue(header.nonDefaultFirstAcquirer());

        assertFalse(header.isDefault());
    }

    @Test
    public void testSetFirstAcquirerFromPrimitive() {
        AmqpHeader header = new AmqpHeader();

        header.setFirstAcquirer(false);

        assertFalse(header.isFirstAcquirer());
        assertFalse(header.nonDefaultFirstAcquirer());

        assertTrue(header.isDefault());

        header.setFirstAcquirer(true);

        assertTrue(header.isFirstAcquirer());
        assertTrue(header.nonDefaultFirstAcquirer());

        assertFalse(header.isDefault());
    }

    //----- Test Delivery Count Property ---------------------------------------//

    @Test
    public void testSetDeliveryCountFromNull() {
        AmqpHeader header = new AmqpHeader();

        header.setDeliveryCount((UnsignedInteger) null);

        assertEquals(0, header.getDeliveryCount());
        assertFalse(header.nonDefaultDeliveryCount());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetDeliveryCountFromUnsigedInteger() {
        AmqpHeader header = new AmqpHeader();

        header.setDeliveryCount(UnsignedInteger.valueOf((byte) 90));

        assertEquals(90, header.getDeliveryCount());
        assertTrue(header.nonDefaultDeliveryCount());

        assertFalse(header.isDefault());

        header.setDeliveryCount(UnsignedInteger.valueOf((byte) 0));

        assertEquals(0, header.getDeliveryCount());
        assertFalse(header.nonDefaultDeliveryCount());

        assertTrue(header.isDefault());
    }

    @Test
    public void testSetDeliveryCountFromPrimitive() {
        AmqpHeader header = new AmqpHeader();

        header.setDeliveryCount(9);

        assertEquals(9, header.getDeliveryCount());
        assertTrue(header.nonDefaultDeliveryCount());

        assertFalse(header.isDefault());

        header.setDeliveryCount(0);

        assertEquals(0, header.getDeliveryCount());
        assertFalse(header.nonDefaultDeliveryCount());

        assertTrue(header.isDefault());
    }
}
