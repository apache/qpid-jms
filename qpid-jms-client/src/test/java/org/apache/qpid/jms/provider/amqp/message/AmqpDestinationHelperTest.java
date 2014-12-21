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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.QUEUE_ATTRIBUTE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.QUEUE_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TEMPORARY_ATTRIBUTE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TEMP_QUEUE_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TEMP_TOPIC_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TOPIC_ATTRIBUTE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TOPIC_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTemporaryQueue;
import org.apache.qpid.jms.JmsTemporaryTopic;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpDestinationHelperTest {
    public static final String TEMP_QUEUE_ATTRIBUTES_STRING = QUEUE_ATTRIBUTE + "," + TEMPORARY_ATTRIBUTE;
    public static final String TEMP_TOPIC_ATTRIBUTES_STRING = TOPIC_ATTRIBUTE + "," + TEMPORARY_ATTRIBUTE;

    private final AmqpDestinationHelper helper = AmqpDestinationHelper.INSTANCE;

    //--------------- Test getJmsDestination method --------------------------//

    @Test
    public void testGetJmsDestinationWithNullAddressAndNullConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(null);
        Mockito.when(message.getMessageAnnotation(
            TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTE);

        assertNull(helper.getJmsDestination(message, null));
    }

    @Test
    public void testGetJmsDestinationWithNullAddressWithConsumerDestReturnsSameConsumerDestObject() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(null);
        Mockito.when(message.getMessageAnnotation(
            TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTE);

        JmsDestination consumerDestination = new JmsQueue("ConsumerDestination");
        assertSame(consumerDestination, helper.getJmsDestination(message, consumerDestination));

        consumerDestination = new JmsTopic("ConsumerDestination");
        assertSame(consumerDestination, helper.getJmsDestination(message, consumerDestination));
    }

    @Test
    public void testGetJmsDestinationWithEmptyTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn("");
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = helper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithUnknownTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn("jms.queue");
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = helper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithAnonymousConsumerDest() {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);

        JmsDestination consumerDestination = Mockito.mock(JmsDestination.class);
        Mockito.when(consumerDestination.getName()).thenReturn("ConsumerDestination");

        JmsDestination destination = helper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = helper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithTopicConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
        JmsDestination consumerDestination = new JmsTopic("ConsumerDestination");

        JmsDestination destination = helper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithTempQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
        JmsDestination consumerDestination = new JmsTemporaryQueue("ConsumerDestination");

        JmsDestination destination = helper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithTempTopicConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
        JmsDestination consumerDestination = new JmsTemporaryTopic("ConsumerDestination");

        JmsDestination destination = helper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithQueueTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTE);

        JmsDestination destination = helper.getJmsDestination(message, null);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithTopicTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TOPIC_ATTRIBUTE);

        JmsDestination destination = helper.getJmsDestination(message, null);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithTempQueueTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TEMP_QUEUE_ATTRIBUTES_STRING);

        JmsDestination destination = helper.getJmsDestination(message, null);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsDestinationWithTempTopicTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TEMP_TOPIC_ATTRIBUTES_STRING);

        JmsDestination destination = helper.getJmsDestination(message, null);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    //--------------- Test getJmsReplyTo method ------------------------------//

    @Test
    public void testGetJmsReplyToWithNullAddressAndNullConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(null);
        Mockito.when(message.getMessageAnnotation(
            REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTE);

        assertNull(helper.getJmsDestination(message, null));
    }

    @Test
    public void testGetJmsReplyToWithNullAddressWithConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(null);
        Mockito.when(message.getMessageAnnotation(
            REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTE);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        assertNull(helper.getJmsReplyTo(message, consumerDestination));
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = helper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithAnonymousConsumerDest() {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);

        JmsDestination consumerDestination = Mockito.mock(JmsDestination.class);
        Mockito.when(consumerDestination.getName()).thenReturn("ConsumerDestination");

        JmsDestination destination = helper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplyToWithEmptyTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn("");
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = helper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplyToWithUnknownTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn("jms.queue");
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = helper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithTopicConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
        JmsTopic consumerDestination = new JmsTopic("ConsumerDestination");

        JmsDestination destination = helper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithTempQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
        JmsTemporaryQueue consumerDestination = new JmsTemporaryQueue("ConsumerDestination");

        JmsDestination destination = helper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithTempTopicConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
        JmsTemporaryTopic consumerDestination = new JmsTemporaryTopic("ConsumerDestination");

        JmsDestination destination = helper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplToWithQueueTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTE);

        JmsDestination destination = helper.getJmsReplyTo(message, null);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplToWithTopicTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TOPIC_ATTRIBUTE);

        JmsDestination destination = helper.getJmsReplyTo(message, null);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplToWithTempQueueTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TEMP_QUEUE_ATTRIBUTES_STRING);

        JmsDestination destination = helper.getJmsReplyTo(message, null);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    @Test
    public void testGetJmsReplToWithTempTopicTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TEMP_TOPIC_ATTRIBUTES_STRING);

        JmsDestination destination = helper.getJmsReplyTo(message, null);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getName());
    }

    //--------------- Test setToAddressFromDestination method ----------------//

    @Test
    public void testSetToAddressFromDestinationWithNullDestination() {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        helper.setToAddressFromDestination(message, null);
        Mockito.verify(message).setToAddress(null);
        Mockito.verify(message).removeMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
    }

    @Test(expected=NullPointerException.class)
    public void testSetToAddressFromDestinationWithNullDestinationAndNullMessage() {
        helper.setToAddressFromDestination(null, null);
    }

    @Test(expected=NullPointerException.class)
    public void testSetToAddressFromDestinationWithNullMessage() {
        JmsDestination destination = new JmsQueue("testAddress");
        helper.setToAddressFromDestination(null, destination);
    }

    @Test
    public void testSetToAddressFromDestinationWithQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).setMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, QUEUE_TYPE);
    }

    @Test
    public void testSetToAddressFromDestinationWithTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).setMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TOPIC_TYPE);
    }

    @Test
    public void testSetToAddressFromDestinationWithTempQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).setMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TEMP_QUEUE_TYPE);
    }

    @Test
    public void testSetToAddressFromDestinationWithTempTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).setMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TEMP_TOPIC_TYPE);
    }

    @Test
    public void testSetToAddressFromDestinationWithAnonymousDestination() {
        String testAddress = "testAddress";
        JmsDestination destination = Mockito.mock(JmsDestination.class);
        Mockito.when(destination.getName()).thenReturn(testAddress);
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
    }

    //--------------- Test setReplyToAddressFromDestination method -----------//

    @Test
    public void testSetReplyToAddressFromDestinationWithNullDestination() {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        helper.setReplyToAddressFromDestination(message, null);
        Mockito.verify(message).setReplyToAddress(null);
        Mockito.verify(message).removeMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
    }

    @Test(expected=NullPointerException.class)
    public void testSetReplyToAddressFromDestinationWithNullDestinationAndNullMessage() {
        helper.setReplyToAddressFromDestination(null, null);
    }

    @Test(expected=NullPointerException.class)
    public void testSetReplyToAddressFromDestinationWithNullMessage() {
        JmsDestination destination = new JmsQueue("testAddress");
        helper.setReplyToAddressFromDestination(null, destination);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).setMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, QUEUE_TYPE);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).setMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TOPIC_TYPE);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTempQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).setMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TEMP_QUEUE_TYPE);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTempTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).setMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TEMP_TOPIC_TYPE);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithAnonymousDestination() {
        String testAddress = "testAddress";
        JmsDestination destination = Mockito.mock(JmsDestination.class);
        Mockito.when(destination.getName()).thenReturn(testAddress);
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
    }

    //--------------- Test Support Methods -----------------------------------//

    @Test
    public void testSplitAttributeWithExtraneousCommas() throws Exception {

        Set<String> set = new HashSet<String>();
        set.add(AmqpDestinationHelper.QUEUE_ATTRIBUTE);
        set.add(AmqpDestinationHelper.TEMPORARY_ATTRIBUTE);

        // test for no NPE errors.
        assertNull(helper.splitAttributesString(null));

        // test a single comma separator produces expected set
        assertEquals(set, helper.splitAttributesString(QUEUE_ATTRIBUTE + "," +
                                                 AmqpDestinationHelper.TEMPORARY_ATTRIBUTE));

        // test trailing comma doesn't alter produced set
        assertEquals(set, helper.splitAttributesString(QUEUE_ATTRIBUTE + "," +
                                                 AmqpDestinationHelper.TEMPORARY_ATTRIBUTE + ","));

        // test leading comma doesn't alter produced set
        assertEquals(set, helper.splitAttributesString("," + QUEUE_ATTRIBUTE + ","
                                                     + AmqpDestinationHelper.TEMPORARY_ATTRIBUTE));

        // test consecutive central commas don't alter produced set
        assertEquals(set, helper.splitAttributesString(QUEUE_ATTRIBUTE + ",," +
                                                 AmqpDestinationHelper.TEMPORARY_ATTRIBUTE));

        // test consecutive trailing commas don't alter produced set
        assertEquals(set, helper.splitAttributesString(QUEUE_ATTRIBUTE + "," +
                                                 AmqpDestinationHelper.TEMPORARY_ATTRIBUTE + ",,"));

        // test consecutive leading commas don't alter produced set
        assertEquals(set, helper.splitAttributesString("," + QUEUE_ATTRIBUTE + ","
                                                     + AmqpDestinationHelper.TEMPORARY_ATTRIBUTE));
    }
}
