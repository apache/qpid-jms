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
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

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

        //TODO: this test will need to expand for classification of receiver type in future
        assertTrue(dest instanceof DestinationImpl);
    }

    @Test
    public void testDecodeDestinationWithQueueTypeAnnotation() throws Exception
    {
        String testAddress = "testAddress";
        String testTypeAnnotation = "queue";

        Destination dest = _helper.decodeDestination(testAddress, testTypeAnnotation);
        assertNotNull(dest);
        assertTrue(dest instanceof DestinationImpl);
        assertTrue(dest instanceof QueueImpl);
        assertTrue(dest instanceof Queue);
        assertEquals(testAddress, ((Queue) dest).getQueueName());
    }

    @Test
    public void testDecodeDestinationWithTopicTypeAnnotation() throws Exception
    {
        String testAddress = "testAddress";
        String testTypeAnnotation = "topic";

        Destination dest = _helper.decodeDestination(testAddress, testTypeAnnotation);
        assertNotNull(dest);
        assertTrue(dest instanceof DestinationImpl);
        assertTrue(dest instanceof TopicImpl);
        assertTrue(dest instanceof Topic);
        assertEquals(testAddress, ((Topic) dest).getTopicName());
    }

    @Test
    public void testDecodeDestinationWithTempTopicTypeAnnotationThrowsIAE() throws Exception
    {
        //TODO: complete implementation when TempTopics implemented
        String testAddress = "testAddress";
        String testTypeAnnotation = "topic,temporary";
        String testTypeAnnotationBackwards = "temporary,topic";

        try
        {
            Destination dest = _helper.decodeDestination(testAddress, testTypeAnnotation);
            fail("expected exceptionnow thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        try
        {
            Destination dest = _helper.decodeDestination(testAddress, testTypeAnnotationBackwards);
            fail("expected exceptionnow thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    @Test
    public void testDecodeDestinationWithTempQueueTypeAnnotationThrowsIAE() throws Exception
    {
        //TODO: complete implementation when TempQueues implemented
        String testAddress = "testAddress";
        String testTypeAnnotation = "queue,temporary";
        String testTypeAnnotationBackwards = "temporary,queue";

        try
        {
            Destination dest = _helper.decodeDestination(testAddress, testTypeAnnotation);
            fail("expected exceptionnow thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        try
        {
            Destination dest = _helper.decodeDestination(testAddress, testTypeAnnotationBackwards);
            fail("expected exceptionnow thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    @Test
    public void testConvertToQpidDestinationWithNullReturnsNull() throws Exception
    {
        assertNull(_helper.convertToQpidDestination(null));
    }

    @Test
    public void testConvertToQpidDestinationWithQpidDestinationReturnsSameObject() throws Exception
    {
        String testAddress = "testAddress";
        Queue queue = _helper.createQueue(testAddress);

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
    public void testConvertToQpidDestinationWithNonQpidTopic() throws Exception
    {
        String testAddress = "testAddress";
        Topic mockTopic = Mockito.mock(Topic.class);
        Mockito.when(mockTopic.getTopicName()).thenReturn(testAddress);

        Destination dest = _helper.convertToQpidDestination(mockTopic);
        assertNotNull(dest);
        assertTrue(dest instanceof Topic);
        assertEquals(testAddress, ((Topic)dest).getTopicName());
    }

    @Test
    public void testConvertToQpidDestinationWithNonQpidTempQueueThrowsIAE() throws Exception
    {
        //TODO: complete implementation when TempQueues implemented
        String testAddress = "testAddress";
        TemporaryQueue mockTempQueue = Mockito.mock(TemporaryQueue.class);
        Mockito.when(mockTempQueue.getQueueName()).thenReturn(testAddress);

        try
        {
            Destination dest = _helper.convertToQpidDestination(mockTempQueue);
            fail("excepted exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    @Test
    public void testConvertToQpidDestinationWithNonQpidTempTopic() throws Exception
    {
        //TODO: complete implementation when TempTopics implemented
        String testAddress = "testAddress";
        TemporaryTopic mockTempTopic = Mockito.mock(TemporaryTopic.class);
        Mockito.when(mockTempTopic.getTopicName()).thenReturn(testAddress);

        try
        {
            Destination dest = _helper.convertToQpidDestination(mockTempTopic);
            fail("excepted exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    @Test
    public void testDecodeAddressWithNullReturnsNull() throws Exception
    {
        assertNull(_helper.decodeAddress(null));
    }

    @Test
    public void testDecodeAddressWithQpidQueue() throws Exception
    {
        String testAddress = "testAddress";
        Queue queue = _helper.createQueue(testAddress);

        assertEquals(testAddress, _helper.decodeAddress(queue));
    }

    @Test
    public void testDecodeAddressWithNonQpidQueueReturnsConvertedAddress() throws Exception
    {
        String testAddress = "testAddress";
        Queue mockQueue = Mockito.mock(Queue.class);
        Mockito.when(mockQueue.getQueueName()).thenReturn(testAddress);

        assertEquals(testAddress, _helper.decodeAddress(mockQueue));
    }

    @Test
    public void testSplitAttributeWithExtranerousCommas() throws Exception
    {
        Set<String> set = new HashSet<String>();
        set.add(DestinationHelper.QUEUE_ATTRIBUTE);
        set.add(DestinationHelper.TEMPORARY_ATTRIBUTE);

        //test a single comma separator produces expected set
        assertEquals(set, _helper.splitAttributes(DestinationHelper.QUEUE_ATTRIBUTES_STRING
                                                  + ","
                                                  + DestinationHelper.TEMPORARY_ATTRIBUTE));

        //test trailing comma doesn't alter produced set
        assertEquals(set, _helper.splitAttributes(DestinationHelper.QUEUE_ATTRIBUTES_STRING
                                                  + ","
                                                  + DestinationHelper.TEMPORARY_ATTRIBUTE
                                                  + ","));
        //test leading comma doesn't alter produced set
        assertEquals(set, _helper.splitAttributes(","
                                                  + DestinationHelper.QUEUE_ATTRIBUTES_STRING
                                                  + ","
                                                  + DestinationHelper.TEMPORARY_ATTRIBUTE));
        //test consecutive central commas don't alter produced set
        assertEquals(set, _helper.splitAttributes(DestinationHelper.QUEUE_ATTRIBUTES_STRING
                                                  + ",,"
                                                  + DestinationHelper.TEMPORARY_ATTRIBUTE));
        //test consecutive trailing commas don't alter produced set
        assertEquals(set, _helper.splitAttributes(DestinationHelper.QUEUE_ATTRIBUTES_STRING
                                                  + ","
                                                  + DestinationHelper.TEMPORARY_ATTRIBUTE
                                                  + ",,"));
        //test consecutive leading commas don't alter produced set
        assertEquals(set, _helper.splitAttributes(","
                                                  + DestinationHelper.QUEUE_ATTRIBUTES_STRING
                                                  + ","
                                                  + DestinationHelper.TEMPORARY_ATTRIBUTE));
    }
}
