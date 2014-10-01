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

import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.QUEUE_ATTRIBUTES_STRING;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TEMP_QUEUE_ATTRIBUTES_STRING;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TEMP_TOPIC_ATTRIBUTES_STRING;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TOPIC_ATTRIBUTES_STRING;
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

    private final AmqpDestinationHelper helper = AmqpDestinationHelper.INSTANCE;

    private static final String QUEUE_PREFIX = "queue://";
    private static final String TOPIC_PREFIX = "topic://";
    private static final String TEMP_QUEUE_PREFIX = "temp-queue://";
    private static final String TEMP_TOPIC_PREFIX = "temp-topic://";

    private AmqpConnection createConnectionWithDestinationPrefixValues() {
        AmqpConnection connection = Mockito.mock(AmqpConnection.class);
        Mockito.when(connection.getQueuePrefix()).thenReturn(QUEUE_PREFIX);
        Mockito.when(connection.getTopicPrefix()).thenReturn(TOPIC_PREFIX);
        Mockito.when(connection.getTempQueuePrefix()).thenReturn(TEMP_QUEUE_PREFIX);
        Mockito.when(connection.getTempTopicPrefix()).thenReturn(TEMP_TOPIC_PREFIX);

        return connection;
    }

    //--------------- Test createDestination method --------------------------//

    @Test(expected=NullPointerException.class)
    public void testCreateDestinationFromStringWithNullConnection() {
        helper.createDestination("testName", null);
    }

    @Test(expected=NullPointerException.class)
    public void testCreateDestinationFromNullStringWithConnection() {
        helper.createDestination(null, createConnectionWithDestinationPrefixValues());
    }

    @Test(expected=NullPointerException.class)
    public void testCreateDestinationFromNullStringAndConnection() {
        helper.createDestination(null, null);
    }

    @Test
    public void testCreateDestinationFromStringNoPrefixReturnsQueue() {
        String destinationName = "testDestinationName";
        AmqpConnection connection = createConnectionWithDestinationPrefixValues();
        JmsDestination result = helper.createDestination(destinationName, connection);
        assertNotNull(result);
        assertTrue(result.isQueue());
        assertFalse(result.isTemporary());
        assertEquals(destinationName, result.getName());
    }

    @Test
    public void testCreateDestinationFromQeueuePrefixedString() {
        String destinationName = "testDestinationName";
        AmqpConnection connection = createConnectionWithDestinationPrefixValues();
        JmsDestination result = helper.createDestination(QUEUE_PREFIX + destinationName, connection);
        assertNotNull(result);
        assertTrue(result.isQueue());
        assertFalse(result.isTemporary());
        assertEquals(destinationName, result.getName());
    }

    @Test
    public void testCreateDestinationFromTopicPrefixedString() {
        String destinationName = "testDestinationName";
        AmqpConnection connection = createConnectionWithDestinationPrefixValues();
        JmsDestination result = helper.createDestination(TOPIC_PREFIX + destinationName, connection);
        assertNotNull(result);
        assertTrue(result.isTopic());
        assertFalse(result.isTemporary());
        assertEquals(destinationName, result.getName());
    }

    @Test
    public void testCreateDestinationFromTempQueuePrefixedString() {
        String destinationName = "testDestinationName";
        AmqpConnection connection = createConnectionWithDestinationPrefixValues();
        JmsDestination result = helper.createDestination(TEMP_QUEUE_PREFIX + destinationName, connection);
        assertNotNull(result);
        assertTrue(result.isQueue());
        assertTrue(result.isTemporary());
        assertEquals(destinationName, result.getName());
    }

    @Test
    public void testCreateDestinationFromTempTopicPrefixedString() {
        String destinationName = "testDestinationName";
        AmqpConnection connection = createConnectionWithDestinationPrefixValues();
        JmsDestination result = helper.createDestination(TEMP_TOPIC_PREFIX + destinationName, connection);
        assertNotNull(result);
        assertTrue(result.isTopic());
        assertTrue(result.isTemporary());
        assertEquals(destinationName, result.getName());
    }

    //--------------- Test getJmsDestination method --------------------------//

    @Test
    public void testGetJmsDestinationWithNullAddressAndNullConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(null);
        Mockito.when(message.getAnnotation(
            TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTES_STRING);

        assertNull(helper.getJmsDestination(message, null));
    }

    @Test
    public void testGetJmsDestinationWithNullAddressWithConsumerDestReturnsSameConsumerDestObject() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(null);
        Mockito.when(message.getAnnotation(
            TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTES_STRING);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        assertSame(consumerDestination, helper.getJmsDestination(message, consumerDestination));
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
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
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
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
        Mockito.when(message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
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
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
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
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTES_STRING);

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
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TOPIC_ATTRIBUTES_STRING);

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
        Mockito.when(message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TEMP_QUEUE_ATTRIBUTES_STRING);

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
        Mockito.when(message.getAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TEMP_TOPIC_ATTRIBUTES_STRING);

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
        Mockito.when(message.getAnnotation(
            REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTES_STRING);

        assertNull(helper.getJmsDestination(message, null));
    }

    @Test
    public void testGetJmsReplyToWithNullAddressWithConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(null);
        Mockito.when(message.getAnnotation(
            REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTES_STRING);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        assertNull(helper.getJmsReplyTo(message, consumerDestination));
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
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
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
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
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
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
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(null);
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
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTES_STRING);

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
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TOPIC_ATTRIBUTES_STRING);

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
        Mockito.when(message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TEMP_QUEUE_ATTRIBUTES_STRING);

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
        Mockito.when(message.getAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(TEMP_TOPIC_ATTRIBUTES_STRING);

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
        Mockito.verify(message).removeAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
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

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).setAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, QUEUE_ATTRIBUTES_STRING);
    }

    @Test
    public void testSetToAddressFromDestinationWithTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).setAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TOPIC_ATTRIBUTES_STRING);
    }

    @Test
    public void testSetToAddressFromDestinationWithTempQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).setAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TEMP_QUEUE_ATTRIBUTES_STRING);
    }

    @Test
    public void testSetToAddressFromDestinationWithTempTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).setAnnotation(TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TEMP_TOPIC_ATTRIBUTES_STRING);
    }

    //--------------- Test setReplyToAddressFromDestination method -----------//

    @Test
    public void testSetReplyToAddressFromDestinationWithNullDestination() {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        helper.setReplyToAddressFromDestination(message, null);
        Mockito.verify(message).setReplyToAddress(null);
        Mockito.verify(message).removeAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
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

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).setAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, QUEUE_ATTRIBUTES_STRING);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).setAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TOPIC_ATTRIBUTES_STRING);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTempQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).setAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TEMP_QUEUE_ATTRIBUTES_STRING);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTempTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        helper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).setAnnotation(REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, TEMP_TOPIC_ATTRIBUTES_STRING);
    }

    //--------------- Test Support Methods -----------------------------------//

    @Test
    public void testSplitAttributeWithExtranerousCommas() throws Exception {

        Set<String> set = new HashSet<String>();
        set.add(AmqpDestinationHelper.QUEUE_ATTRIBUTE);
        set.add(AmqpDestinationHelper.TEMPORARY_ATTRIBUTE);

        // test a single comma separator produces expected set
        assertEquals(set, helper.splitAttributes(AmqpDestinationHelper.QUEUE_ATTRIBUTES_STRING + "," +
                                                 AmqpDestinationHelper.TEMPORARY_ATTRIBUTE));

        // test trailing comma doesn't alter produced set
        assertEquals(set, helper.splitAttributes(AmqpDestinationHelper.QUEUE_ATTRIBUTES_STRING + "," +
                                                 AmqpDestinationHelper.TEMPORARY_ATTRIBUTE + ","));

        // test leading comma doesn't alter produced set
        assertEquals(set, helper.splitAttributes("," + AmqpDestinationHelper.QUEUE_ATTRIBUTES_STRING + ","
                                                     + AmqpDestinationHelper.TEMPORARY_ATTRIBUTE));

        // test consecutive central commas don't alter produced set
        assertEquals(set, helper.splitAttributes(AmqpDestinationHelper.QUEUE_ATTRIBUTES_STRING + ",," +
                                                 AmqpDestinationHelper.TEMPORARY_ATTRIBUTE));

        // test consecutive trailing commas don't alter produced set
        assertEquals(set, helper.splitAttributes(AmqpDestinationHelper.QUEUE_ATTRIBUTES_STRING + "," +
                                                 AmqpDestinationHelper.TEMPORARY_ATTRIBUTE + ",,"));

        // test consecutive leading commas don't alter produced set
        assertEquals(set, helper.splitAttributes("," + AmqpDestinationHelper.QUEUE_ATTRIBUTES_STRING + ","
                                                     + AmqpDestinationHelper.TEMPORARY_ATTRIBUTE));
    }
}
