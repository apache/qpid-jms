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
package org.apache.qpid.jms.integration;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.ApplicationPropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;

public class JMSProducerIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    private Symbol[] SERVER_ANONYMOUS_RELAY = new Symbol[]{ANONYMOUS_RELAY};

    private static final String NULL_STRING_PROP = "nullStringProperty";
    private static final String NULL_STRING_PROP_VALUE = null;
    private static final String STRING_PROP = "stringProperty";
    private static final String STRING_PROP_VALUE = "string";
    private static final String BOOLEAN_PROP = "booleanProperty";
    private static final boolean BOOLEAN_PROP_VALUE = true;
    private static final String BYTE_PROP = "byteProperty";
    private static final byte   BYTE_PROP_VALUE = (byte)1;
    private static final String SHORT_PROP = "shortProperty";
    private static final short  SHORT_PROP_VALUE = (short)1;
    private static final String INT_PROP = "intProperty";
    private static final int    INT_PROP_VALUE = Integer.MAX_VALUE;
    private static final String LONG_PROP = "longProperty";
    private static final long   LONG_PROP_VALUE = Long.MAX_VALUE;
    private static final String FLOAT_PROP = "floatProperty";
    private static final float  FLOAT_PROP_VALUE = Float.MAX_VALUE;
    private static final String DOUBLE_PROP = "doubleProperty";
    private static final double DOUBLE_PROP_VALUE = Double.MAX_VALUE;

    @Test(timeout = 20000)
    public void testCreateProducerAndSend() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, SERVER_ANONYMOUS_RELAY);
            testPeer.expectBegin();

            //Expect a link to the anonymous relay node
            TargetMatcher targetMatcher = new TargetMatcher();
            targetMatcher.withAddress(nullValue());
            targetMatcher.withDynamic(nullValue());//default = false
            targetMatcher.withDurable(nullValue());//default = none/0
            testPeer.expectSenderAttach(targetMatcher, false, false);

            JMSProducer producer = context.createProducer();
            assertNotNull(producer);

            String topicName = "testCreateProducerAndSend";
            Topic topic = context.createTopic(topicName);

            // Verify sent message contains expected destination address and dest type annotation
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            Symbol annotationKey = AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL;
            msgAnnotationsMatcher.withEntry(annotationKey, equalTo(AmqpDestinationHelper.TOPIC_TYPE));

            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withTo(equalTo(topicName));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true).withDurable(equalTo(true)));
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);

            testPeer.expectTransfer(messageMatcher);

            Message message = context.createMessage();
            producer.send(topic, message);

            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testJMSProducerHasDefaultConfiguration() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, SERVER_ANONYMOUS_RELAY);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            JMSProducer producer = context.createProducer();
            assertNotNull(producer);

            assertEquals(Message.DEFAULT_DELIVERY_DELAY, producer.getDeliveryDelay());
            assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
            assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
            assertEquals(Message.DEFAULT_TIME_TO_LIVE, producer.getTimeToLive());

            testPeer.expectEnd();
            testPeer.expectClose();
            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testJMSProducerSetPropertySendsApplicationProperties() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, SERVER_ANONYMOUS_RELAY);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            String queueName = "myQueue";
            Queue queue = context.createQueue(queueName);
            JMSProducer producer = context.createProducer();

            ApplicationPropertiesSectionMatcher appPropsMatcher = new ApplicationPropertiesSectionMatcher(true);
            appPropsMatcher.withEntry(NULL_STRING_PROP, nullValue());
            appPropsMatcher.withEntry(STRING_PROP, equalTo(STRING_PROP_VALUE));
            appPropsMatcher.withEntry(BOOLEAN_PROP, equalTo(BOOLEAN_PROP_VALUE));
            appPropsMatcher.withEntry(BYTE_PROP, equalTo(BYTE_PROP_VALUE));
            appPropsMatcher.withEntry(SHORT_PROP, equalTo(SHORT_PROP_VALUE));
            appPropsMatcher.withEntry(INT_PROP, equalTo(INT_PROP_VALUE));
            appPropsMatcher.withEntry(LONG_PROP, equalTo(LONG_PROP_VALUE));
            appPropsMatcher.withEntry(FLOAT_PROP, equalTo(FLOAT_PROP_VALUE));
            appPropsMatcher.withEntry(DOUBLE_PROP, equalTo(DOUBLE_PROP_VALUE));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withTo(equalTo(queueName));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setApplicationPropertiesMatcher(appPropsMatcher);
            testPeer.expectTransfer(messageMatcher);

            producer.setProperty(NULL_STRING_PROP, NULL_STRING_PROP_VALUE);
            producer.setProperty(STRING_PROP, STRING_PROP_VALUE);
            producer.setProperty(BOOLEAN_PROP, BOOLEAN_PROP_VALUE);
            producer.setProperty(BYTE_PROP, BYTE_PROP_VALUE);
            producer.setProperty(SHORT_PROP, SHORT_PROP_VALUE);
            producer.setProperty(INT_PROP, INT_PROP_VALUE);
            producer.setProperty(LONG_PROP, LONG_PROP_VALUE);
            producer.setProperty(FLOAT_PROP, FLOAT_PROP_VALUE);
            producer.setProperty(DOUBLE_PROP, DOUBLE_PROP_VALUE);

            producer.send(queue, "test");

            testPeer.expectEnd();
            testPeer.expectClose();

            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testJMSProducerPropertyOverridesMessageValue() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JMSContext context = testFixture.createJMSContext(testPeer, SERVER_ANONYMOUS_RELAY);
            testPeer.expectBegin();
            testPeer.expectSenderAttach();

            String queueName = "myQueue";
            Queue queue = context.createQueue(queueName);
            Message message = context.createMessage();
            JMSProducer producer = context.createProducer();

            ApplicationPropertiesSectionMatcher appPropsMatcher = new ApplicationPropertiesSectionMatcher(true);
            appPropsMatcher.withEntry(STRING_PROP, equalTo(STRING_PROP_VALUE));

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            MessagePropertiesSectionMatcher propsMatcher = new MessagePropertiesSectionMatcher(true).withTo(equalTo(queueName));

            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setApplicationPropertiesMatcher(appPropsMatcher);
            testPeer.expectTransfer(messageMatcher);

            message.setStringProperty(STRING_PROP, "ThisShouldNotBeTransmitted");
            producer.setProperty(STRING_PROP, STRING_PROP_VALUE);
            producer.send(queue, message);

            testPeer.expectEnd();
            testPeer.expectClose();

            context.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
