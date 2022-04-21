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
package org.apache.qpid.jms.tracing.opentracing;

import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.ANNOTATION_KEY;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.COMPONENT;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.DELIVERY_SETTLED;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.ERROR_EVENT;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.MESSAGE_EXPIRED;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.ONMESSAGE_SPAN_NAME;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.RECEIVE_SPAN_NAME;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.REDELIVERIES_EXCEEDED;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.SEND_SPAN_NAME;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.STATE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Connection;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.HeaderDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AcceptedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.ModifiedMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.log.Fields;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockSpan.LogEntry;
import io.opentracing.mock.MockSpan.MockContext;
import io.opentracing.mock.MockTracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;

public class OpenTracingIntegrationTest extends QpidJmsTestCase {

    @Test(timeout = 20000)
    public void testSend() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            testPeer.expectSenderAttach();

            MessageProducer producer = session.createProducer(queue);

            // Expect a message with the trace info annotation set
            String msgContent = "myTracedMessageContent";
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(ANNOTATION_KEY), Matchers.any(Map.class));
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(msgContent));

            testPeer.expectTransfer(messageMatcher);

            TextMessage message = session.createTextMessage(msgContent);
            producer.send(message);

            testPeer.waitForAllHandlersToComplete(2000);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
            Span sendSpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, sendSpan.getClass());
            MockSpan sendMockSpan = (MockSpan) sendSpan;

            assertEquals("Expected span to have no parent", 0, sendMockSpan.parentId());
            assertEquals("Unexpected span operation name", SEND_SPAN_NAME, sendMockSpan.operationName());

            // Verify tags set on the completed span
            Map<String, Object> spanTags = sendMockSpan.tags();
            assertFalse("Expected some tags", spanTags.isEmpty());
            assertFalse("Expected error tag not to be set", spanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_PRODUCER, spanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));

            // Verify log set on the completed span
            List<LogEntry> entries = sendMockSpan.logEntries();
            assertEquals("Expected 1 log entry: " + entries, 1, entries.size());

            Map<String, ?> entryFields = entries.get(0).fields();
            assertFalse("Expected some log entry fields", entryFields.isEmpty());
            assertNotNull("Expected a state description", entryFields.get(STATE));
            assertEquals(DELIVERY_SETTLED, entryFields.get(Fields.EVENT));

            // Verify the context sent on the wire matches the original span
            Object obj = msgAnnotationsMatcher.getReceivedAnnotation(Symbol.valueOf(ANNOTATION_KEY));
            assertTrue("annotation was not a map", obj instanceof Map);
            @SuppressWarnings("unchecked")
            Map<String, String> traceInfo = (Map<String, String>) obj;
            assertFalse("Expected some content in map", traceInfo.isEmpty());

            SpanContext extractedContext = mockTracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(traceInfo));
            assertEquals("Unexpected context class", MockContext.class, extractedContext.getClass());
            assertEquals("Extracted context spanId did not match original", sendMockSpan.context().spanId(), ((MockContext) extractedContext).spanId());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testSendPreSettled() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer, "jms.presettlePolicy.presettleProducers=true"));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            testPeer.expectSettledSenderAttach();

            MessageProducer producer = session.createProducer(queue);

            // Expect a message with the trace info annotation set
            String msgContent = "myTracedMessageContent";
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(new MessageHeaderSectionMatcher(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(Symbol.valueOf(ANNOTATION_KEY), Matchers.any(Map.class));
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(new MessagePropertiesSectionMatcher(true));
            messageMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(msgContent));

            // Expect settled transfer
            testPeer.expectTransfer(messageMatcher, Matchers.nullValue(), true, false, null, false);

            TextMessage message = session.createTextMessage(msgContent);
            producer.send(message);

            // Await the pre-settled transfer completing (so we can get some details of it from the peer) and span finishing.
            testPeer.waitForAllHandlersToComplete(2000);
            boolean finishedSpanFound = Wait.waitFor(() -> !(mockTracer.finishedSpans().isEmpty()), 3000, 10);
            assertTrue("Did not get finished span after send", finishedSpanFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
            Span sendSpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, sendSpan.getClass());
            MockSpan sendMockSpan = (MockSpan) sendSpan;

            assertEquals("Expected span to have no parent", 0, sendMockSpan.parentId());
            assertEquals("Unexpected span operation name", SEND_SPAN_NAME, sendMockSpan.operationName());

            // Verify tags set on the completed span
            Map<String, Object> spanTags = sendMockSpan.tags();
            assertFalse("Expected some tags", spanTags.isEmpty());
            assertFalse("Expected error tag not to be set", spanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_PRODUCER, spanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));

            // Verify log set on the completed span
            List<LogEntry> entries = sendMockSpan.logEntries();
            assertEquals("Expected 1 log entry: " + entries, 1, entries.size());

            Map<String, ?> entryFields = entries.get(0).fields();
            assertFalse("Expected some log entry fields", entryFields.isEmpty());
            assertNotNull("Expected a state description", entryFields.get(STATE));
            assertEquals(DELIVERY_SETTLED, entryFields.get(Fields.EVENT));

            // Verify the context sent on the wire matches the original span
            Object obj = msgAnnotationsMatcher.getReceivedAnnotation(Symbol.valueOf(ANNOTATION_KEY));
            assertTrue("annotation was not a map", obj instanceof Map);
            @SuppressWarnings("unchecked")
            Map<String, String> traceInfo = (Map<String, String>) obj;
            assertFalse("Expected some content in map", traceInfo.isEmpty());

            SpanContext extractedContext = mockTracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(traceInfo));
            assertEquals("Unexpected context class", MockContext.class, extractedContext.getClass());
            assertEquals("Extracted context spanId did not match original", sendMockSpan.context().spanId(), ((MockContext) extractedContext).spanId());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testReceive() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            // Prepare an arriving message with tracing info
            Map<String,String> injected = new HashMap<>();
            MockSpan sendSpan = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected));
            assertFalse("Expected inject to add values", injected.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected);

            String msgContent = "myContent";
            DescribedType amqpValueContent = new AmqpValueDescribedType(msgContent);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message msg = messageConsumer.receive(2000);

            assertNotNull("Did not receive message as expected", msg);
            assertNull("expected no active span", mockTracer.activeSpan());

            boolean finishedSpanFound = Wait.waitFor(() -> !(mockTracer.finishedSpans().isEmpty()), 3000, 10);
            assertTrue("Did not get finished span after receive", finishedSpanFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
            Span deliverySpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            assertEquals("Expected span to be child of the send span", sendSpan.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags set on the completed span
            Map<String, Object> spanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", spanTags.isEmpty());
            assertFalse("Expected error tag not to be set", spanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log set on the completed span
            List<LogEntry> logEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + logEntries, logEntries.isEmpty());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveBody() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            JMSContext context = factory.createContext();
            context.start();

            testPeer.expectBegin();

            String queueName = "myQueue";
            Queue queue = context.createQueue(queueName);

            // Prepare an arriving message with tracing info
            Map<String,String> injected = new HashMap<>();
            MockSpan sendSpan = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected));
            assertFalse("Expected inject to add values", injected.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected);

            String msgContent = "myContent";
            DescribedType amqpValueContent = new AmqpValueDescribedType(msgContent);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            JMSConsumer consumer = context.createConsumer(queue);

            String body = consumer.receiveBody(String.class, 2000);

            assertEquals("Did not receive message body as expected", msgContent, body);
            assertNull("expected no active span", mockTracer.activeSpan());

            boolean finishedSpanFound = Wait.waitFor(() -> !(mockTracer.finishedSpans().isEmpty()), 3000, 10);
            assertTrue("Did not get finished span after receiveBody", finishedSpanFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
            Span deliverySpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            assertEquals("Expected span to be child of the send span", sendSpan.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags set on the completed span
            Map<String, Object> spanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", spanTags.isEmpty());
            assertFalse("Expected error tag not to be set", spanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log set on the completed span
            List<LogEntry> logEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + logEntries, logEntries.isEmpty());

            testPeer.expectEnd();
            testPeer.expectClose();

            context.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }


    @Test(timeout = 20000)
    public void testReceiveWithoutTraceInfo() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            // Prepare an arriving message without tracing info
            String msgContent = "myContent";
            DescribedType amqpValueContent = new AmqpValueDescribedType(msgContent);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message msg = messageConsumer.receive(2000);

            assertNotNull("Did not receive message as expected", msg);
            assertNull("expected no active span", mockTracer.activeSpan());

            boolean finishedSpanFound = Wait.waitFor(() -> !(mockTracer.finishedSpans().isEmpty()), 3000, 10);
            assertTrue("Did not get finished span after receive", finishedSpanFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
            Span deliverySpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            assertEquals("Expected span to have no parent as incoming message had no context", 0, deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags set on the completed span
            Map<String, Object> spanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", spanTags.isEmpty());
            assertFalse("Expected error tag not to be set", spanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log set on the completed span
            List<LogEntry> logEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + logEntries, logEntries.isEmpty());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveWithExpiredMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            // Prepare an arriving message with tracing info, but which has also already expired
            Map<String,String> injected1 = new HashMap<>();
            MockSpan sendSpan1 = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan1.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected1));
            assertFalse("Expected inject to add values", injected1.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations1 = new MessageAnnotationsDescribedType();
            msgAnnotations1.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected1);

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 100));

            String expiredMsgContent = "already-expired";

            // Also prepare a message which is not expired yet.
            String liveMsgContent = "still-active";

            Map<String,String> injected2 = new HashMap<>();
            MockSpan sendSpan2 = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan2.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected2));
            assertFalse("Expected inject to add values", injected2.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations2 = new MessageAnnotationsDescribedType();
            msgAnnotations2.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected2);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations1, props, null, new AmqpValueDescribedType(expiredMsgContent));

            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, msgAnnotations2, null, null, new AmqpValueDescribedType(liveMsgContent), 2);

            ModifiedMatcher modified = new ModifiedMatcher();
            modified.withDeliveryFailed(equalTo(true));
            modified.withUndeliverableHere(equalTo(true));

            testPeer.expectDisposition(true, modified, 1, 1);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message msg = messageConsumer.receive(3000);

            assertNotNull("Message should have been received", msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("Unexpected message content", liveMsgContent, ((TextMessage)msg).getText());
            assertNotEquals(expiredMsgContent, liveMsgContent);

            assertNull("expected no active span", mockTracer.activeSpan());

            boolean finishedSpansFound = Wait.waitFor(() -> (mockTracer.finishedSpans().size() == 2), 3000, 10);
            assertTrue("Did not get finished spans after receive", finishedSpansFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 2 finished spans: " + finishedSpans, 2, finishedSpans.size());

            Span expiredSpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, expiredSpan.getClass());
            MockSpan expiredMockSpan = (MockSpan) expiredSpan;

            assertEquals("Expected expired message span to be child of the first send span", sendSpan1.context().spanId(), expiredMockSpan.parentId());
            assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, expiredMockSpan.operationName());

            // Verify tags on the span for expired message
            Map<String, Object> expiredSpanTags = expiredMockSpan.tags();
            assertFalse("Expected some tags", expiredSpanTags.isEmpty());
            assertFalse("Expected error tag not to be set", expiredSpanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, expiredSpanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, expiredSpanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, expiredSpanTags.get(Tags.COMPONENT.getKey()));

            // Verify log on the span for expired message
            List<LogEntry> expiredLogEntries = expiredMockSpan.logEntries();
            assertEquals("Expected 1 log entry: " + expiredLogEntries, 1, expiredLogEntries.size());
            Map<String, ?> entryFields = expiredLogEntries.get(0).fields();
            assertFalse("Expected some log entry fields", entryFields.isEmpty());
            assertEquals(MESSAGE_EXPIRED, entryFields.get(Fields.EVENT));

            Span deliverySpan = finishedSpans.get(1);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            assertEquals("Expected delivery span to be child of the second send span", sendSpan2.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags on the span for delivered message
            Map<String, Object> deliveredSpanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", deliveredSpanTags.isEmpty());
            assertFalse("Expected error tag not to be set", deliveredSpanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, deliveredSpanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, deliveredSpanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, deliveredSpanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log on the span for delivered message
            List<LogEntry> deliveredLogEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + deliveredLogEntries, deliveredLogEntries.isEmpty());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);

            sendSpan1.finish();
            sendSpan2.finish();
            finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 4 finished spans: " + finishedSpans, 4, finishedSpans.size());
        }
    }

    @Test(timeout = 20000)
    public void testReceiveWithRedeliveryPolicy() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer, "jms.redeliveryPolicy.maxRedeliveries=1"));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            // Prepare an arriving message with tracing info, but which has also already exceeded the redelivery-policy
            Map<String,String> injected1 = new HashMap<>();
            MockSpan sendSpan1 = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan1.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected1));
            assertFalse("Expected inject to add values", injected1.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations1 = new MessageAnnotationsDescribedType();
            msgAnnotations1.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected1);

            HeaderDescribedType header = new HeaderDescribedType();
            header.setDeliveryCount(UnsignedInteger.valueOf(2));

            String redeliveredMsgContent = "already-exceeded-redelivery-policy";

            // Also prepare a message which has not exceeded the redelivery policy yet.
            String liveMsgContent = "still-active";

            Map<String,String> injected2 = new HashMap<>();
            MockSpan sendSpan2 = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan2.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected2));
            assertFalse("Expected inject to add values", injected2.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations2 = new MessageAnnotationsDescribedType();
            msgAnnotations2.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected2);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(header, msgAnnotations1, null, null, new AmqpValueDescribedType(redeliveredMsgContent));

            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, msgAnnotations2, null, null, new AmqpValueDescribedType(liveMsgContent), 2);

            ModifiedMatcher modified = new ModifiedMatcher();
            modified.withDeliveryFailed(equalTo(true));
            modified.withUndeliverableHere(equalTo(true));

            testPeer.expectDisposition(true, modified, 1, 1);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message msg = messageConsumer.receive(3000);

            assertNotNull("Message should have been received", msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("Unexpected message content", liveMsgContent, ((TextMessage)msg).getText());
            assertNotEquals(redeliveredMsgContent, liveMsgContent);

            assertNull("expected no active span", mockTracer.activeSpan());

            boolean finishedSpansFound = Wait.waitFor(() -> (mockTracer.finishedSpans().size() == 2), 3000, 10);
            assertTrue("Did not get finished spans after receive", finishedSpansFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 2 finished spans: " + finishedSpans, 2, finishedSpans.size());

            Span redeliveredSpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, redeliveredSpan.getClass());
            MockSpan redeliveredMockSpan = (MockSpan) redeliveredSpan;

            assertEquals("Expected redelivered message span to be child of the first send span", sendSpan1.context().spanId(), redeliveredMockSpan.parentId());
            assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, redeliveredMockSpan.operationName());

            // Verify tags on the span for redelivered message
            Map<String, Object> redeliveredSpanTags = redeliveredMockSpan.tags();
            assertFalse("Expected some tags", redeliveredSpanTags.isEmpty());
            assertFalse("Expected error tag not to be set", redeliveredSpanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, redeliveredSpanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, redeliveredSpanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, redeliveredSpanTags.get(Tags.COMPONENT.getKey()));

            // Verify log on the span for redelivered message
            List<LogEntry> redeliveredLogEntries = redeliveredMockSpan.logEntries();
            assertEquals("Expected 1 log entry: " + redeliveredLogEntries, 1, redeliveredLogEntries.size());
            Map<String, ?> entryFields = redeliveredLogEntries.get(0).fields();
            assertFalse("Expected some log entry fields", entryFields.isEmpty());
            assertEquals(REDELIVERIES_EXCEEDED, entryFields.get(Fields.EVENT));

            Span deliverySpan = finishedSpans.get(1);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            assertEquals("Expected delivery span to be child of the second send span", sendSpan2.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags on the span for delivered message
            Map<String, Object> deliveredSpanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", deliveredSpanTags.isEmpty());
            assertFalse("Expected error tag not to be set", deliveredSpanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, deliveredSpanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, deliveredSpanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, deliveredSpanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log on the span for delivered message
            List<LogEntry> deliveredLogEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + deliveredLogEntries, deliveredLogEntries.isEmpty());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);

            sendSpan1.finish();
            sendSpan2.finish();
            finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 4 finished spans: " + finishedSpans, 4, finishedSpans.size());
        }
    }

    @Test(timeout = 20000)
    public void testOnMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            // Prepare an arriving message with tracing info
            Map<String,String> injected = new HashMap<>();
            MockSpan sendSpan = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected));
            assertFalse("Expected inject to add values", injected.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected);

            String msgContent = "myContent";
            DescribedType amqpValueContent = new AmqpValueDescribedType(msgContent);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            AtomicReference<Span> activeSpanRef = new AtomicReference<>();
            AtomicReference<Throwable> throwableRef = new AtomicReference<>();
            CountDownLatch deliveryRun = new CountDownLatch(1);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        activeSpanRef.set(mockTracer.activeSpan());

                        deliveryRun.countDown();
                    } catch (Throwable t) {
                        throwableRef.set(t);
                    }
                }
            });

            assertTrue("onMessage did not run in timely fashion: " + throwableRef.get(), deliveryRun.await(3000, TimeUnit.MILLISECONDS));

            Span deliverySpan = activeSpanRef.get();
            assertNotNull("expected an active span during onMessage", deliverySpan);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            boolean finishedSpanFound = Wait.waitFor(() -> !(mockTracer.finishedSpans().isEmpty()), 3000, 10);
            assertTrue("Did not get finished span after onMessage", finishedSpanFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
            assertEquals("Unexpected finished span", deliverySpan, finishedSpans.get(0));

            assertEquals("Expected span to be child of the send span", sendSpan.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags set on the completed span
            Map<String, Object> spanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", spanTags.isEmpty());
            assertFalse("Expected error tag not to be set", spanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log set on the completed span
            List<LogEntry> logEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + logEntries, logEntries.isEmpty());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);

            sendSpan.finish();
            finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 2 finished spans: " + finishedSpans, 2, finishedSpans.size());

            assertNull("Unexpected error during onMessage", throwableRef.get());
        }
    }

    @Test(timeout = 20000)
    public void testOnMessageWithoutTraceInfo() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            // Prepare an arriving message without tracing info
            String msgContent = "myContent";
            DescribedType amqpValueContent = new AmqpValueDescribedType(msgContent);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            AtomicReference<Span> activeSpanRef = new AtomicReference<>();
            AtomicReference<Throwable> throwableRef = new AtomicReference<>();
            CountDownLatch deliveryRun = new CountDownLatch(1);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        activeSpanRef.set(mockTracer.activeSpan());
                        deliveryRun.countDown();
                    } catch (Throwable t) {
                        throwableRef.set(t);
                    }
                }
            });

            assertTrue("onMessage did not run in timely fashion: " + throwableRef.get(), deliveryRun.await(3000, TimeUnit.MILLISECONDS));

            Span deliverySpan = activeSpanRef.get();
            assertNotNull("expected an active span during onMessage", deliverySpan);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            boolean finishedSpanFound = Wait.waitFor(() -> !(mockTracer.finishedSpans().isEmpty()), 3000, 10);
            assertTrue("Did not get finished span after onMessage", finishedSpanFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
            assertEquals("Unexpected finished span", deliverySpan, finishedSpans.get(0));

            assertEquals("Expected span to have no parent as incoming message had no context", 0, deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags set on the completed span
            Map<String, Object> spanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", spanTags.isEmpty());
            assertFalse("Expected error tag not to be set", spanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log set on the completed span
            List<LogEntry> logEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + logEntries, logEntries.isEmpty());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);

            assertNull("Unexpected error during onMessage", throwableRef.get());
        }
    }

    @Test(timeout = 20000)
    public void testOnMessageThrowingException() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            // Prepare an arriving message with tracing info
            Map<String,String> injected = new HashMap<>();
            MockSpan sendSpan = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected));
            assertFalse("Expected inject to add values", injected.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected);

            String msgContent = "myContent";
            DescribedType amqpValueContent = new AmqpValueDescribedType(msgContent);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, amqpValueContent);
            testPeer.expectDispositionThatIsReleasedAndSettled();

            AtomicReference<Span> activeSpanRef = new AtomicReference<>();
            AtomicReference<Throwable> throwableRef = new AtomicReference<>();
            CountDownLatch deliveryRun = new CountDownLatch(1);

            String exceptionMessage = "not-supposed-to-throw-from-onMessage";
            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        activeSpanRef.set(mockTracer.activeSpan());

                        deliveryRun.countDown();
                    } catch (Throwable t) {
                        throwableRef.set(t);
                    }

                    throw new RuntimeException(exceptionMessage);
                }
            });

            assertTrue("onMessage did not run in timely fashion: " + throwableRef.get(), deliveryRun.await(3000, TimeUnit.MILLISECONDS));

            Span deliverySpan = activeSpanRef.get();
            assertNotNull("expected an active span during onMessage", deliverySpan);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            boolean finishedSpanFound = Wait.waitFor(() -> !(mockTracer.finishedSpans().isEmpty()), 3000, 10);
            assertTrue("Did not get finished span after onMessage", finishedSpanFound);

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
            assertEquals("Unexpected finished span", deliveryMockSpan, finishedSpans.get(0));

            assertEquals("Expected span to be child of the send span", sendSpan.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags set on the completed span
            Map<String, Object> spanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", spanTags.isEmpty());
            assertTrue("Expected error tag to be true", (Boolean) spanTags.get(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));

            // Verify log set on the completed span
            List<LogEntry> logEntries = deliveryMockSpan.logEntries();
            assertEquals("Expected 1 log entry: " + logEntries, 1, logEntries.size());

            Map<String, ?> entryFields = logEntries.get(0).fields();
            assertFalse("Expected some log entry fields", entryFields.isEmpty());
            assertEquals(ERROR_EVENT, entryFields.get(Fields.EVENT));
            Object messageDesc = entryFields.get(Fields.MESSAGE);
            assertTrue(messageDesc instanceof String);
            assertTrue(((String) messageDesc).contains("thrown from onMessage"));
            Object t = entryFields.get(Fields.ERROR_OBJECT);
            assertNotNull("Expected error object to be set", t);
            assertTrue(t instanceof RuntimeException);
            assertTrue(exceptionMessage.equals(((RuntimeException) t).getMessage()));

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);

            sendSpan.finish();
            finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 2 finished spans: " + finishedSpans, 2, finishedSpans.size());

            assertNull("Unexpected error during onMessage", throwableRef.get());
        }
    }

    @Test(timeout = 20000)
    public void testOnMessageWithExpiredMessage() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            // Prepare an arriving message with tracing info, but which has also already expired
            Map<String,String> injected1 = new HashMap<>();
            MockSpan sendSpan1 = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan1.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected1));
            assertFalse("Expected inject to add values", injected1.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations1 = new MessageAnnotationsDescribedType();
            msgAnnotations1.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected1);

            PropertiesDescribedType props = new PropertiesDescribedType();
            props.setAbsoluteExpiryTime(new Date(System.currentTimeMillis() - 100));

            String expiredMsgContent = "already-expired";

            // Also prepare a message which is not expired yet.
            String liveMsgContent = "still-active";

            Map<String,String> injected2 = new HashMap<>();
            MockSpan sendSpan2 = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan2.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected2));
            assertFalse("Expected inject to add values", injected2.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations2 = new MessageAnnotationsDescribedType();
            msgAnnotations2.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected2);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations1, props, null, new AmqpValueDescribedType(expiredMsgContent));

            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, msgAnnotations2, null, null, new AmqpValueDescribedType(liveMsgContent), 2);

            ModifiedMatcher modified = new ModifiedMatcher();
            modified.withDeliveryFailed(equalTo(true));
            modified.withUndeliverableHere(equalTo(true));

            testPeer.expectDisposition(true, modified, 1, 1);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            AtomicReference<Span> activeSpanRef = new AtomicReference<>();
            AtomicReference<Throwable> throwableRef = new AtomicReference<>();
            CountDownLatch deliveryRun = new CountDownLatch(1);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            messageConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        activeSpanRef.compareAndSet(null, mockTracer.activeSpan());

                        deliveryRun.countDown();
                    } catch (Throwable t) {
                        throwableRef.set(t);
                    }
                }
            });

            assertTrue("onMessage did not run in timely fashion: " + throwableRef.get(), deliveryRun.await(3000, TimeUnit.MILLISECONDS));

            boolean finishedSpansFound = Wait.waitFor(() -> (mockTracer.finishedSpans().size() == 2), 3000, 10);
            assertTrue("Did not get finished spans after receive", finishedSpansFound);

            Span deliverySpan = activeSpanRef.get();
            assertNotNull("expected an active span during onMessage", deliverySpan);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 2 finished spans: " + finishedSpans, 2, finishedSpans.size());

            assertEquals("Expected span to be child of the second send span", sendSpan2.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

            Span expiredSpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, expiredSpan.getClass());
            MockSpan expiredMockSpan = (MockSpan) expiredSpan;

            assertEquals("Expected expired message span to be child of the first send span", sendSpan1.context().spanId(), expiredMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, expiredMockSpan.operationName());

            // Verify tags on the span for expired message
            Map<String, Object> expiredSpanTags = expiredMockSpan.tags();
            assertFalse("Expected some tags", expiredSpanTags.isEmpty());
            assertFalse("Expected error tag not to be set", expiredSpanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, expiredSpanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, expiredSpanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, expiredSpanTags.get(Tags.COMPONENT.getKey()));

            // Verify log on the span for expired message
            List<LogEntry> expiredLogEntries = expiredMockSpan.logEntries();
            assertEquals("Expected 1 log entry: " + expiredLogEntries, 1, expiredLogEntries.size());
            Map<String, ?> entryFields = expiredLogEntries.get(0).fields();
            assertFalse("Expected some log entry fields", entryFields.isEmpty());
            assertEquals(MESSAGE_EXPIRED, entryFields.get(Fields.EVENT));

            assertEquals("Unexpected second finished span", deliveryMockSpan, finishedSpans.get(1));
            assertEquals("Expected delivery span to be child of the second send span", sendSpan2.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags on the span for delivered message
            Map<String, Object> deliveredSpanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", deliveredSpanTags.isEmpty());
            assertFalse("Expected error tag not to be set", deliveredSpanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, deliveredSpanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(queueName, deliveredSpanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, deliveredSpanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log on the span for delivered message
            List<LogEntry> deliveredLogEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + deliveredLogEntries, deliveredLogEntries.isEmpty());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);

            sendSpan1.finish();
            sendSpan2.finish();
            finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 4 finished spans: " + finishedSpans, 4, finishedSpans.size());

            assertNull("Unexpected error during onMessage", throwableRef.get());
        }
    }

    @Test(timeout = 20000)
    public void testOnMessageWithRedeliveryPolicy() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsConnectionFactory factory = new JmsConnectionFactory(createPeerURI(testPeer, "jms.redeliveryPolicy.maxRedeliveries=1"));

            MockTracer mockTracer = new MockTracer();
            JmsTracer tracer = OpenTracingTracerFactory.create(mockTracer);
            factory.setTracer(tracer);

            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String topicName = "myTopic";
            Topic topic = session.createTopic(topicName);

            // Prepare an arriving message with tracing info, but which has also already exceeded the redelivery-policy
            Map<String,String> injected1 = new HashMap<>();
            MockSpan sendSpan1 = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan1.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected1));
            assertFalse("Expected inject to add values", injected1.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations1 = new MessageAnnotationsDescribedType();
            msgAnnotations1.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected1);

            HeaderDescribedType header = new HeaderDescribedType();
            header.setDeliveryCount(UnsignedInteger.valueOf(2));

            String redeliveredMsgContent = "already-exceeded-redelivery-policy";

            // Also prepare a message which has not exceeded the redelivery policy yet.
            String liveMsgContent = "still-active";

            Map<String,String> injected2 = new HashMap<>();
            MockSpan sendSpan2 = mockTracer.buildSpan(SEND_SPAN_NAME).start();
            mockTracer.inject(sendSpan2.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected2));
            assertFalse("Expected inject to add values", injected2.isEmpty());

            MessageAnnotationsDescribedType msgAnnotations2 = new MessageAnnotationsDescribedType();
            msgAnnotations2.setSymbolKeyedAnnotation(ANNOTATION_KEY, injected2);

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(header, msgAnnotations1, null, null, new AmqpValueDescribedType(redeliveredMsgContent));

            testPeer.sendTransferToLastOpenedLinkOnLastOpenedSession(null, msgAnnotations2, null, null, new AmqpValueDescribedType(liveMsgContent), 2);

            ModifiedMatcher modified = new ModifiedMatcher();
            modified.withDeliveryFailed(equalTo(true));
            modified.withUndeliverableHere(equalTo(true));

            testPeer.expectDisposition(true, modified, 1, 1);
            testPeer.expectDisposition(true, new AcceptedMatcher(), 2, 2);

            AtomicReference<Span> activeSpanRef = new AtomicReference<>();
            AtomicReference<Throwable> throwableRef = new AtomicReference<>();
            CountDownLatch deliveryRun = new CountDownLatch(1);

            MessageConsumer messageConsumer = session.createConsumer(topic);
            messageConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        activeSpanRef.compareAndSet(null, mockTracer.activeSpan());

                        deliveryRun.countDown();
                    } catch (Throwable t) {
                        throwableRef.set(t);
                    }
                }
            });

            assertTrue("onMessage did not run in timely fashion: " + throwableRef.get(), deliveryRun.await(3000, TimeUnit.MILLISECONDS));

            boolean finishedSpansFound = Wait.waitFor(() -> (mockTracer.finishedSpans().size() == 2), 3000, 10);
            assertTrue("Did not get finished spans after receive", finishedSpansFound);

            Span deliverySpan = activeSpanRef.get();
            assertNotNull("expected an active span during onMessage", deliverySpan);
            assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
            MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

            List<MockSpan> finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 2 finished spans: " + finishedSpans, 2, finishedSpans.size());

            assertEquals("Expected span to be child of the second send span", sendSpan2.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

            Span redeliveredSpan = finishedSpans.get(0);
            assertEquals("Unexpected span class", MockSpan.class, redeliveredSpan.getClass());
            MockSpan redeliveredMockSpan = (MockSpan) redeliveredSpan;

            assertEquals("Expected redelivered message span to be child of the first send span", sendSpan1.context().spanId(), redeliveredMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, redeliveredMockSpan.operationName());

            // Verify tags on the span for redelivered message
            Map<String, Object> redeliveredSpanTags = redeliveredMockSpan.tags();
            assertFalse("Expected some tags", redeliveredSpanTags.isEmpty());
            assertFalse("Expected error tag not to be set", redeliveredSpanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, redeliveredSpanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(topicName, redeliveredSpanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, redeliveredSpanTags.get(Tags.COMPONENT.getKey()));

            // Verify log on the span for redelivered message
            List<LogEntry> redeliveredLogEntries = redeliveredMockSpan.logEntries();
            assertEquals("Expected 1 log entry: " + redeliveredLogEntries, 1, redeliveredLogEntries.size());
            Map<String, ?> entryFields = redeliveredLogEntries.get(0).fields();
            assertFalse("Expected some log entry fields", entryFields.isEmpty());
            assertEquals(REDELIVERIES_EXCEEDED, entryFields.get(Fields.EVENT));

            assertEquals("Unexpected second finished span", deliveryMockSpan, finishedSpans.get(1));
            assertEquals("Expected delivery span to be child of the second send span", sendSpan2.context().spanId(), deliveryMockSpan.parentId());
            assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

            // Verify tags on the span for delivered message
            Map<String, Object> deliveredSpanTags = deliveryMockSpan.tags();
            assertFalse("Expected some tags", deliveredSpanTags.isEmpty());
            assertFalse("Expected error tag not to be set", deliveredSpanTags.containsKey(Tags.ERROR.getKey()));
            assertEquals(Tags.SPAN_KIND_CONSUMER, deliveredSpanTags.get(Tags.SPAN_KIND.getKey()));
            assertEquals(topicName, deliveredSpanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
            assertEquals(COMPONENT, deliveredSpanTags.get(Tags.COMPONENT.getKey()));

            // Verify no log on the span for delivered message
            List<LogEntry> deliveredLogEntries = deliveryMockSpan.logEntries();
            assertTrue("Expected no log entry: " + deliveredLogEntries, deliveredLogEntries.isEmpty());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(2000);

            sendSpan1.finish();
            sendSpan2.finish();
            finishedSpans = mockTracer.finishedSpans();
            assertEquals("Expected 4 finished spans: " + finishedSpans, 4, finishedSpans.size());

            assertNull("Unexpected error during onMessage", throwableRef.get());
        }
    }

    private String createPeerURI(TestAmqpPeer peer) {
        return createPeerURI(peer, null);
    }

    private String createPeerURI(TestAmqpPeer peer, String params) {
        return "amqp://localhost:" + peer.getServerPort() + (params != null ? "?" + params : "");
    }
}
