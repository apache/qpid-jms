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
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.ARRIVING_SPAN_CTX_CONTEXT_KEY;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.COMPONENT;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.DELIVERY_SETTLED;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.DELIVERY_SPAN_CONTEXT_KEY;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.ONMESSAGE_SCOPE_CONTEXT_KEY;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.ONMESSAGE_SPAN_NAME;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.RECEIVE_SPAN_NAME;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.SEND_SPAN_CONTEXT_KEY;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.SEND_SPAN_NAME;
import static org.apache.qpid.jms.tracing.opentracing.OpenTracingTracer.STATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.jms.tracing.JmsTracer.DeliveryOutcome;
import org.apache.qpid.jms.tracing.TraceableMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockSpan.LogEntry;
import io.opentracing.mock.MockSpan.MockContext;
import io.opentracing.mock.MockTracer;
import io.opentracing.noop.NoopTracerFactory;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;
import io.opentracing.util.ThreadLocalScope;

public class OpenTracingTracerTest extends QpidJmsTestCase {

    @Captor
    private ArgumentCaptor<Map<String, String>> annotationMapCaptor;
    private AutoCloseable closable;

    @BeforeEach
    public void setUp() {
        closable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void tearDown() throws Exception {
        closable.close();
    }

    @Test
    public void testCreateAndCloseOmitUnderlyingClose() {
        // Test when set NOT TO close the underlying Tracer
        Tracer mockTracer2 = Mockito.mock(Tracer.class);
        JmsTracer jmsTracer2  = new OpenTracingTracer(mockTracer2, false);

        Mockito.verifyNoInteractions(mockTracer2);
        jmsTracer2.close();
        Mockito.verifyNoInteractions(mockTracer2);
    }

    @Test
    public void testCreateAndClose() {
        // Test when set TO close the underlying Tracer
        Tracer mockTracer1 = Mockito.mock(Tracer.class);
        JmsTracer jmsTracer1  = new OpenTracingTracer(mockTracer1, true);

        Mockito.verifyNoInteractions(mockTracer1);
        jmsTracer1.close();
        Mockito.verify(mockTracer1).close();
        Mockito.verifyNoMoreInteractions(mockTracer1);
    }

    @Test
    public void testSendOperations() {
        MockTracer mockTracer = new MockTracer();
        JmsTracer jmsTracer  = new OpenTracingTracer(mockTracer, true);
        TraceableMessage message = Mockito.mock(TraceableMessage.class);
        String sendDestination = "myAddress";
        String sendOutcomeDescription = "myOutcomeDescription";

        // Start send operation
        jmsTracer.initSend(message, sendDestination);

        assertNull(mockTracer.activeSpan(), "Did not expect active span to be present");

        ArgumentCaptor<Span> sendSpanCapture = ArgumentCaptor.forClass(Span.class);
        Mockito.verify(message).setTracingContext(Mockito.eq(SEND_SPAN_CONTEXT_KEY), sendSpanCapture.capture());
        Mockito.verify(message).setTracingAnnotation(Mockito.eq(ANNOTATION_KEY), annotationMapCaptor.capture());
        Mockito.verifyNoMoreInteractions(message);

        Span sendSpan = sendSpanCapture.getValue();
        assertNotNull(sendSpan, "expected a span from send operation");
        Mockito.when(message.getTracingContext(SEND_SPAN_CONTEXT_KEY)).thenReturn(sendSpan);
        assertEquals(MockSpan.class, sendSpan.getClass(), "Unexpected span class");
        MockSpan sendMockSpan = (MockSpan) sendSpan;

        assertEquals(SEND_SPAN_NAME, sendMockSpan.operationName(), "Unexpected span operation name");

        Map<String, String> annotationValue = annotationMapCaptor.getValue();
        assertNotNull(annotationValue, "expected an annotation from the send operation");
        Mockito.when(message.getTracingAnnotation(ANNOTATION_KEY)).thenReturn(annotationValue);

        assertTrue(mockTracer.finishedSpans().isEmpty(), "Expected no finished spans");

        // Finish the send operation
        jmsTracer.completeSend(message, sendOutcomeDescription);

        Mockito.verify(message).getTracingContext(Mockito.eq(SEND_SPAN_CONTEXT_KEY));
        Mockito.verifyNoMoreInteractions(message);

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals(1, finishedSpans.size(), "Expected 1 finished span: " + finishedSpans);
        assertEquals(sendSpan, finishedSpans.get(0), "Unexpected finished span");

        // Verify log set on the completed span
        List<LogEntry> entries = sendMockSpan.logEntries();
        assertEquals(1, entries.size(), "Expected 1 log entry: " + entries);

        Map<String, ?> entryFields = entries.get(0).fields();
        assertFalse(entryFields.isEmpty(), "Expected some log entry fields");
        assertEquals(sendOutcomeDescription, entryFields.get(STATE));
        assertEquals(DELIVERY_SETTLED, entryFields.get(Fields.EVENT));

        // Verify tags set on the span
        Map<String, Object> spanTags = sendMockSpan.tags();
        assertFalse(spanTags.isEmpty(), "Expected some tags");
        assertEquals(Tags.SPAN_KIND_PRODUCER, spanTags.get(Tags.SPAN_KIND.getKey()));
        assertEquals(sendDestination, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
        assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));
    }

    @Test
    public void testSendOperationsWithoutTracingContextToSend() {
        // Use the NoOp tracer to ensure there is no context to send
        Tracer noopTracer = NoopTracerFactory.create();
        JmsTracer jmsTracer  = new OpenTracingTracer(noopTracer, true);
        TraceableMessage message = Mockito.mock(TraceableMessage.class);
        String sendDestination = "myAddress";
        String sendOutcomeDescription = "myOutcomeDescription";

        // Start send operation
        jmsTracer.initSend(message, sendDestination);

        // Should have cleared the tracing annotation, if there was no trace context injected for this send.
        Mockito.verify(message).removeTracingAnnotation(Mockito.eq(ANNOTATION_KEY));

        ArgumentCaptor<Span> sendSpanCapture = ArgumentCaptor.forClass(Span.class);
        Mockito.verify(message).setTracingContext(Mockito.eq(SEND_SPAN_CONTEXT_KEY), sendSpanCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        Span sendSpan = sendSpanCapture.getValue();
        assertNotNull(sendSpan, "expected a span from send operation");
        Mockito.when(message.getTracingContext(SEND_SPAN_CONTEXT_KEY)).thenReturn(sendSpan);

        // Finish the send operation
        jmsTracer.completeSend(message, sendOutcomeDescription);

        Mockito.verify(message).getTracingContext(Mockito.eq(SEND_SPAN_CONTEXT_KEY));
        Mockito.verifyNoMoreInteractions(message);
    }

    @Test
    public void testReceiveOperations() {
        MockTracer mockTracer = new MockTracer();
        JmsTracer jmsTracer  = new OpenTracingTracer(mockTracer, true);
        String consumerDestination = "myAddress";

        // Prepare an 'arriving' message with tracing info
        TraceableMessage message = Mockito.mock(TraceableMessage.class);

        Map<String,String> injected = new HashMap<>();
        MockSpan sendSpan = mockTracer.buildSpan(SEND_SPAN_NAME).start();
        mockTracer.inject(sendSpan.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected));

        assertFalse(injected.isEmpty(), "Expected inject to add values");

        Mockito.when(message.getTracingAnnotation(ANNOTATION_KEY)).thenReturn(injected);

        assertTrue(mockTracer.finishedSpans().isEmpty(), "Expected no finished spans");

        // Do the receive operation
        jmsTracer.syncReceive(message, consumerDestination, DeliveryOutcome.DELIVERED);

        ArgumentCaptor<SpanContext> sendSpanContextCapture = ArgumentCaptor.forClass(SpanContext.class);
        Mockito.verify(message).getTracingAnnotation(Mockito.eq(ANNOTATION_KEY));
        Mockito.verify(message).setTracingContext(Mockito.eq(ARRIVING_SPAN_CTX_CONTEXT_KEY), sendSpanContextCapture.capture());

        SpanContext sendContext = sendSpanContextCapture.getValue();
        assertNotNull(sendContext, "expected a span context from extract operation");
        assertEquals(MockContext.class, sendContext.getClass(), "Unexpected context class");
        assertEquals(sendSpan.context().spanId(), ((MockContext) sendContext).spanId(), "Extracted context spanId did not match original");

        ArgumentCaptor<Span> deliverySpanCapture = ArgumentCaptor.forClass(Span.class);
        Mockito.verify(message).setTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY), deliverySpanCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        Span deliverySpan = deliverySpanCapture.getValue();
        assertNotNull(deliverySpan, "expected a span from receive operation");
        assertEquals(MockSpan.class, deliverySpan.getClass(), "Unexpected span class");
        MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

        assertEquals(RECEIVE_SPAN_NAME, deliveryMockSpan.operationName(), "Unexpected span operation name");

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals(1, finishedSpans.size(), "Expected 1 finished span: " + finishedSpans);
        assertEquals(deliverySpan, finishedSpans.get(0), "Unexpected finished span");

        assertEquals(sendSpan.context().spanId(), deliveryMockSpan.parentId(), "Expected span to be child of 'send' span");

        // Verify tags set on the span
        Map<String, Object> spanTags = deliveryMockSpan.tags();
        assertFalse(spanTags.isEmpty(), "Expected some tags");
        assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
        assertEquals(consumerDestination, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
        assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));
    }

    @Test
    public void testReceiveWithoutTracingContext() {
        MockTracer mockTracer = new MockTracer();
        JmsTracer jmsTracer  = new OpenTracingTracer(mockTracer, true);
        String consumerDestination = "myAddress";

        // Prepare an 'arriving' message without tracing info
        TraceableMessage message = Mockito.mock(TraceableMessage.class);
        Mockito.when(message.getTracingAnnotation(ANNOTATION_KEY)).thenReturn(null);

        assertTrue(mockTracer.finishedSpans().isEmpty(), "Expected no finished spans");

        // Do the receive operation, verify behaviour after extract yields no context.
        jmsTracer.syncReceive(message, consumerDestination, DeliveryOutcome.DELIVERED);

        ArgumentCaptor<Span> deliverySpanCapture = ArgumentCaptor.forClass(Span.class);
        Mockito.verify(message).getTracingAnnotation(Mockito.eq(ANNOTATION_KEY));
        Mockito.verify(message).setTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY), deliverySpanCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        Span deliverySpan = deliverySpanCapture.getValue();
        assertNotNull(deliverySpan, "expected a span from receive operation");
        assertEquals(MockSpan.class, deliverySpan.getClass(), "Unexpected span class");
        MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

        assertEquals(RECEIVE_SPAN_NAME, deliveryMockSpan.operationName(), "Unexpected span operation name");

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals(1, finishedSpans.size(), "Expected 1 finished span: " + finishedSpans);
        assertEquals(deliverySpan, finishedSpans.get(0), "Unexpected finished span");

        assertEquals(0, deliveryMockSpan.parentId(), "Expected span to have no parent span");

        // Verify tags set on the span
        Map<String, Object> spanTags = deliveryMockSpan.tags();
        assertFalse(spanTags.isEmpty(), "Expected some tags");
        assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
        assertEquals(consumerDestination, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
        assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));
    }

    @Test
    public void testOnMessageOperations() {
        MockTracer mockTracer = new MockTracer();
        JmsTracer jmsTracer  = new OpenTracingTracer(mockTracer, true);
        String consumerDestination = "myAddress";

        // Prepare an 'arriving' message with tracing info
        TraceableMessage message = Mockito.mock(TraceableMessage.class);

        Map<String,String> injected = new HashMap<>();
        MockSpan sendSpan = mockTracer.buildSpan(SEND_SPAN_NAME).start();
        mockTracer.inject(sendSpan.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(injected));

        assertFalse(injected.isEmpty(), "Expected inject to add values");

        Mockito.when(message.getTracingAnnotation(ANNOTATION_KEY)).thenReturn(injected);

        assertTrue(mockTracer.finishedSpans().isEmpty(), "Expected no finished spans");

        // Do the onMessage init operation
        jmsTracer.asyncDeliveryInit(message, consumerDestination);

        ArgumentCaptor<SpanContext> sendSpanContextCapture = ArgumentCaptor.forClass(SpanContext.class);
        Mockito.verify(message).getTracingAnnotation(Mockito.eq(ANNOTATION_KEY));
        Mockito.verify(message).setTracingContext(Mockito.eq(ARRIVING_SPAN_CTX_CONTEXT_KEY), sendSpanContextCapture.capture());

        SpanContext sendContext = sendSpanContextCapture.getValue();
        assertNotNull(sendContext, "expected a span context from extract operation");
        assertEquals(MockContext.class, sendContext.getClass(), "Unexpected context class");
        assertEquals(sendSpan.context().spanId(), ((MockContext) sendContext).spanId(), "Extracted context did not match original");

        assertTrue(mockTracer.finishedSpans().isEmpty(), "Expected no finished spans");

        ArgumentCaptor<Span> deliverySpanCapture = ArgumentCaptor.forClass(Span.class);
        ArgumentCaptor<Scope> deliveryScopeCapture = ArgumentCaptor.forClass(Scope.class);
        Mockito.verify(message).setTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY), deliverySpanCapture.capture());
        Mockito.verify(message).setTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY), deliveryScopeCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        assertNotNull(mockTracer.activeSpan(), "Expected active span to be present");

        Span deliverySpan = deliverySpanCapture.getValue();
        assertNotNull(deliverySpan, "expected a span from onMessage operation");
        assertEquals(MockSpan.class, deliverySpan.getClass(), "Unexpected span class");
        MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

        assertEquals(ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName(), "Unexpected span operation name");

        Scope deliveryScope = deliveryScopeCapture.getValue();
        assertNotNull(deliveryScope, "expected a scope from onMessage operation");
        assertEquals(ThreadLocalScope.class, deliveryScope.getClass(), "Unexpected scope class");

        Mockito.when(message.getTracingContext(DELIVERY_SPAN_CONTEXT_KEY)).thenReturn(deliverySpan);
        Mockito.when(message.removeTracingContext(ONMESSAGE_SCOPE_CONTEXT_KEY)).thenReturn(deliveryScope);

        assertTrue(mockTracer.finishedSpans().isEmpty(), "Expected no finished spans");

        // Do the onMessage completion operation
        jmsTracer.asyncDeliveryComplete(message, DeliveryOutcome.DELIVERED, null);

        Mockito.verify(message).getTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY));
        Mockito.verify(message).removeTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY));
        Mockito.verifyNoMoreInteractions(message);

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals(1, finishedSpans.size(), "Expected 1 finished span: " + finishedSpans);
        assertEquals(deliverySpan, finishedSpans.get(0), "Unexpected finished span");

        assertEquals(sendSpan.context().spanId(), deliveryMockSpan.parentId(), "Expected span to be child of 'send' span");

        // Verify tags set on the span
        Map<String, Object> spanTags = deliveryMockSpan.tags();
        assertFalse(spanTags.isEmpty(), "Expected some tags");
        assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
        assertEquals(consumerDestination, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
        assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));
    }

    @Test
    public void testOnMessageCompletionClosesScopeAndSpan() {
        MockTracer mockTracer = new MockTracer();
        JmsTracer jmsTracer  = new OpenTracingTracer(mockTracer, true);

        // Prepare a message with tracing context
        TraceableMessage message = Mockito.mock(TraceableMessage.class);
        Span deliverySpan = Mockito.mock(Span.class);
        Scope deliveryScope = Mockito.mock(Scope.class);

        Mockito.when(message.getTracingContext(DELIVERY_SPAN_CONTEXT_KEY)).thenReturn(deliverySpan);
        Mockito.when(message.removeTracingContext(ONMESSAGE_SCOPE_CONTEXT_KEY)).thenReturn(deliveryScope);

        // Do the onMessage completion operation
        jmsTracer.asyncDeliveryComplete(message, DeliveryOutcome.DELIVERED, null);

        Mockito.verify(message).getTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY));
        Mockito.verify(message).removeTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY));
        Mockito.verifyNoMoreInteractions(message);

        // Verify the span and scope are closed
        Mockito.verify(deliverySpan).finish();
        Mockito.verifyNoMoreInteractions(deliverySpan);

        Mockito.verify(deliveryScope).close();
        Mockito.verifyNoMoreInteractions(deliveryScope);
    }

    @Test
    public void testOnMessageOperationsWithoutTracingContext() {
        MockTracer mockTracer = new MockTracer();
        JmsTracer jmsTracer  = new OpenTracingTracer(mockTracer, true);
        String consumerDestination = "myAddress";

        // Prepare an 'arriving' message without the tracing info
        TraceableMessage message = Mockito.mock(TraceableMessage.class);

        Mockito.when(message.getTracingAnnotation(ANNOTATION_KEY)).thenReturn(null);

        assertTrue(mockTracer.finishedSpans().isEmpty(), "Expected no finished spans");
        assertNull(mockTracer.activeSpan(), "Did not expect active span to be present");

        // Do the onMessage init operation, verify behaviour after extract yields no context.
        jmsTracer.asyncDeliveryInit(message, consumerDestination);

        ArgumentCaptor<Span> deliverySpanCapture = ArgumentCaptor.forClass(Span.class);
        ArgumentCaptor<Scope> deliveryScopeCapture = ArgumentCaptor.forClass(Scope.class);
        Mockito.verify(message).getTracingAnnotation(Mockito.eq(ANNOTATION_KEY));
        Mockito.verify(message).setTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY), deliverySpanCapture.capture());
        Mockito.verify(message).setTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY), deliveryScopeCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        assertNotNull(mockTracer.activeSpan(), "Expected active span to be present");

        Span deliverySpan = deliverySpanCapture.getValue();
        assertNotNull(deliverySpan, "expected a span from onMessage operation");
        assertEquals(MockSpan.class, deliverySpan.getClass(), "Unexpected span class");
        MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

        assertEquals(ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName(), "Unexpected span operation name");

        Scope deliveryScope = deliveryScopeCapture.getValue();
        assertNotNull(deliveryScope, "expected a scope from onMessage operation");
        assertEquals(ThreadLocalScope.class, deliveryScope.getClass(), "Unexpected scope class");

        Mockito.when(message.getTracingContext(DELIVERY_SPAN_CONTEXT_KEY)).thenReturn(deliverySpan);
        Mockito.when(message.removeTracingContext(ONMESSAGE_SCOPE_CONTEXT_KEY)).thenReturn(deliveryScope);

        assertTrue(mockTracer.finishedSpans().isEmpty(), "Expected no finished spans");

        // Do the onMessage completion operation
        jmsTracer.asyncDeliveryComplete(message, DeliveryOutcome.DELIVERED, null);

        Mockito.verify(message).getTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY));
        Mockito.verify(message).removeTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY));
        Mockito.verifyNoMoreInteractions(message);

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals(1, finishedSpans.size(), "Expected 1 finished span: " + finishedSpans);
        assertEquals(deliverySpan, finishedSpans.get(0), "Unexpected finished span");

        assertEquals(0, deliveryMockSpan.parentId(), "Expected span to have no parent span");

        // Verify tags set on the span
        Map<String, Object> spanTags = deliveryMockSpan.tags();
        assertFalse(spanTags.isEmpty(), "Expected some tags");
        assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
        assertEquals(consumerDestination, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
        assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));
    }
}
