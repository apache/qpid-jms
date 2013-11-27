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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import javax.jms.Destination;
import javax.jms.Queue;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DestinationHelperTest extends QpidJmsTestCase
{
    private DestinationHelper _helper;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _helper = new DestinationHelper();
    }

    @Test
    public void testDecodeDestinationWithoutTypeAnnotation() throws Exception
    {
        String testAddress = "testAddress";
        Destination dest = _helper.decodeDestination(testAddress, null);
        assertNotNull(dest);

        //TODO: this probably wont be true in future
        assertTrue(dest instanceof Queue);
    }

    @Test
    public void testConvertToQpidDestinationWithNull() throws Exception
    {
        assertNull(_helper.convertToQpidDestination(null));
    }

    @Test
    public void testConvertToQpidDestinationWithQpidDestination() throws Exception
    {
        String testAddress = "testAddress";
        Queue queue = new QueueImpl(testAddress);

        Destination dest = _helper.convertToQpidDestination(queue);
        assertNotNull(dest);
        assertSame(queue, dest);
    }

    @Test
    public void testConvertToQpidDestinationWithNonQpidQueue() throws Exception
    {
        String testAddress = "testAddress";
        Queue mockQueue = Mockito.mock(Queue.class);
        Mockito.when(mockQueue.getQueueName()).thenReturn(testAddress);

        Destination dest = _helper.convertToQpidDestination(mockQueue);
        assertNotNull(dest);
        assertTrue(dest instanceof Queue);
        assertEquals(testAddress, ((Queue)dest).getQueueName());
    }

    @Test
    public void testDecodeAddressWithNull() throws Exception
    {
        assertNull(_helper.decodeAddress(null));
    }

    @Test
    public void testDecodeAddressWithQpidQueue() throws Exception
    {
        String testAddress = "testAddress";
        Queue queue = new QueueImpl(testAddress);

        assertEquals(testAddress, _helper.decodeAddress(queue));
    }

    @Test
    public void testDecodeAddressWithNonQpidQueue() throws Exception
    {
        String testAddress = "testAddress";
        Queue mockQueue = Mockito.mock(Queue.class);
        Mockito.when(mockQueue.getQueueName()).thenReturn(testAddress);

        assertEquals(testAddress, _helper.decodeAddress(mockQueue));
    }
}
