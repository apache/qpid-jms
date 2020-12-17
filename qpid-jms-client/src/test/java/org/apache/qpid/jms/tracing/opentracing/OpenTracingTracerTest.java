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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.jms.tracing.JmsTracer.DeliveryOutcome;
import org.apache.qpid.jms.tracing.TraceableMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

    @Before
    public void setUp() {
        closable = MockitoAnnotations.openMocks(this);
    }

    @After
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

        assertNull("Did not expect active span to be present", mockTracer.activeSpan());

        ArgumentCaptor<Span> sendSpanCapture = ArgumentCaptor.forClass(Span.class);
        Mockito.verify(message).setTracingContext(Mockito.eq(SEND_SPAN_CONTEXT_KEY), sendSpanCapture.capture());
        Mockito.verify(message).setTracingAnnotation(Mockito.eq(ANNOTATION_KEY), annotationMapCaptor.capture());
        Mockito.verifyNoMoreInteractions(message);

        Span sendSpan = sendSpanCapture.getValue();
        assertNotNull("expected a span from send operation", sendSpan);
        Mockito.when(message.getTracingContext(SEND_SPAN_CONTEXT_KEY)).thenReturn(sendSpan);
        assertEquals("Unexpected span class", MockSpan.class, sendSpan.getClass());
        MockSpan sendMockSpan = (MockSpan) sendSpan;

        assertEquals("Unexpected span operation name", SEND_SPAN_NAME, sendMockSpan.operationName());

        Map<String, String> annotationValue = annotationMapCaptor.getValue();
        assertNotNull("expected an annotation from the send operation", annotationValue);
        Mockito.when(message.getTracingAnnotation(ANNOTATION_KEY)).thenReturn(annotationValue);

        assertTrue("Expected no finished spans", mockTracer.finishedSpans().isEmpty());

        // Finish the send operation
        jmsTracer.completeSend(message, sendOutcomeDescription);

        Mockito.verify(message).getTracingContext(Mockito.eq(SEND_SPAN_CONTEXT_KEY));
        Mockito.verifyNoMoreInteractions(message);

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
        assertEquals("Unexpected finished span", sendSpan, finishedSpans.get(0));

        // Verify log set on the completed span
        List<LogEntry> entries = sendMockSpan.logEntries();
        assertEquals("Expected 1 log entry: " + entries, 1, entries.size());

        Map<String, ?> entryFields = entries.get(0).fields();
        assertFalse("Expected some log entry fields", entryFields.isEmpty());
        assertEquals(sendOutcomeDescription, entryFields.get(STATE));
        assertEquals(DELIVERY_SETTLED, entryFields.get(Fields.EVENT));

        // Verify tags set on the span
        Map<String, Object> spanTags = sendMockSpan.tags();
        assertFalse("Expected some tags", spanTags.isEmpty());
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
        assertNotNull("expected a span from send operation", sendSpan);
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

        assertFalse("Expected inject to add values", injected.isEmpty());

        Mockito.when(message.getTracingAnnotation(ANNOTATION_KEY)).thenReturn(injected);

        assertTrue("Expected no finished spans", mockTracer.finishedSpans().isEmpty());

        // Do the receive operation
        jmsTracer.syncReceive(message, consumerDestination, DeliveryOutcome.DELIVERED);

        ArgumentCaptor<SpanContext> sendSpanContextCapture = ArgumentCaptor.forClass(SpanContext.class);
        Mockito.verify(message).getTracingAnnotation(Mockito.eq(ANNOTATION_KEY));
        Mockito.verify(message).setTracingContext(Mockito.eq(ARRIVING_SPAN_CTX_CONTEXT_KEY), sendSpanContextCapture.capture());

        SpanContext sendContext = sendSpanContextCapture.getValue();
        assertNotNull("expected a span context from extract operation", sendContext);
        assertEquals("Unexpected context class", MockContext.class, sendContext.getClass());
        assertEquals("Extracted context spanId did not match original", sendSpan.context().spanId(), ((MockContext) sendContext).spanId());

        ArgumentCaptor<Span> deliverySpanCapture = ArgumentCaptor.forClass(Span.class);
        Mockito.verify(message).setTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY), deliverySpanCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        Span deliverySpan = deliverySpanCapture.getValue();
        assertNotNull("expected a span from receive operation", deliverySpan);
        assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
        MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

        assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, deliveryMockSpan.operationName());

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
        assertEquals("Unexpected finished span", deliverySpan, finishedSpans.get(0));

        assertEquals("Expected span to be child of 'send' span", sendSpan.context().spanId(), deliveryMockSpan.parentId());

        // Verify tags set on the span
        Map<String, Object> spanTags = deliveryMockSpan.tags();
        assertFalse("Expected some tags", spanTags.isEmpty());
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

        assertTrue("Expected no finished spans", mockTracer.finishedSpans().isEmpty());

        // Do the receive operation, verify behaviour after extract yields no context.
        jmsTracer.syncReceive(message, consumerDestination, DeliveryOutcome.DELIVERED);

        ArgumentCaptor<Span> deliverySpanCapture = ArgumentCaptor.forClass(Span.class);
        Mockito.verify(message).getTracingAnnotation(Mockito.eq(ANNOTATION_KEY));
        Mockito.verify(message).setTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY), deliverySpanCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        Span deliverySpan = deliverySpanCapture.getValue();
        assertNotNull("expected a span from receive operation", deliverySpan);
        assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
        MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

        assertEquals("Unexpected span operation name", RECEIVE_SPAN_NAME, deliveryMockSpan.operationName());

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
        assertEquals("Unexpected finished span", deliverySpan, finishedSpans.get(0));

        assertEquals("Expected span to have no parent span", 0, deliveryMockSpan.parentId());

        // Verify tags set on the span
        Map<String, Object> spanTags = deliveryMockSpan.tags();
        assertFalse("Expected some tags", spanTags.isEmpty());
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

        assertFalse("Expected inject to add values", injected.isEmpty());

        Mockito.when(message.getTracingAnnotation(ANNOTATION_KEY)).thenReturn(injected);

        assertTrue("Expected no finished spans", mockTracer.finishedSpans().isEmpty());

        // Do the onMessage init operation
        jmsTracer.asyncDeliveryInit(message, consumerDestination);

        ArgumentCaptor<SpanContext> sendSpanContextCapture = ArgumentCaptor.forClass(SpanContext.class);
        Mockito.verify(message).getTracingAnnotation(Mockito.eq(ANNOTATION_KEY));
        Mockito.verify(message).setTracingContext(Mockito.eq(ARRIVING_SPAN_CTX_CONTEXT_KEY), sendSpanContextCapture.capture());

        SpanContext sendContext = sendSpanContextCapture.getValue();
        assertNotNull("expected a span context from extract operation", sendContext);
        assertEquals("Unexpected context class", MockContext.class, sendContext.getClass());
        assertEquals("Extracted context did not match original", sendSpan.context().spanId(), ((MockContext) sendContext).spanId());

        assertTrue("Expected no finished spans", mockTracer.finishedSpans().isEmpty());

        ArgumentCaptor<Span> deliverySpanCapture = ArgumentCaptor.forClass(Span.class);
        ArgumentCaptor<Scope> deliveryScopeCapture = ArgumentCaptor.forClass(Scope.class);
        Mockito.verify(message).setTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY), deliverySpanCapture.capture());
        Mockito.verify(message).setTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY), deliveryScopeCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        assertNotNull("Expected active span to be present", mockTracer.activeSpan());

        Span deliverySpan = deliverySpanCapture.getValue();
        assertNotNull("expected a span from onMessage operation", deliverySpan);
        assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
        MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

        assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

        Scope deliveryScope = deliveryScopeCapture.getValue();
        assertNotNull("expected a scope from onMessage operation", deliveryScope);
        assertEquals("Unexpected scope class", ThreadLocalScope.class, deliveryScope.getClass());

        Mockito.when(message.getTracingContext(DELIVERY_SPAN_CONTEXT_KEY)).thenReturn(deliverySpan);
        Mockito.when(message.removeTracingContext(ONMESSAGE_SCOPE_CONTEXT_KEY)).thenReturn(deliveryScope);

        assertTrue("Expected no finished spans", mockTracer.finishedSpans().isEmpty());

        // Do the onMessage completion operation
        jmsTracer.asyncDeliveryComplete(message, DeliveryOutcome.DELIVERED, null);

        Mockito.verify(message).getTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY));
        Mockito.verify(message).removeTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY));
        Mockito.verifyNoMoreInteractions(message);

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
        assertEquals("Unexpected finished span", deliverySpan, finishedSpans.get(0));

        assertEquals("Expected span to be child of 'send' span", sendSpan.context().spanId(), deliveryMockSpan.parentId());

        // Verify tags set on the span
        Map<String, Object> spanTags = deliveryMockSpan.tags();
        assertFalse("Expected some tags", spanTags.isEmpty());
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

        assertTrue("Expected no finished spans", mockTracer.finishedSpans().isEmpty());
        assertNull("Did not expect active span to be present", mockTracer.activeSpan());

        // Do the onMessage init operation, verify behaviour after extract yields no context.
        jmsTracer.asyncDeliveryInit(message, consumerDestination);

        ArgumentCaptor<Span> deliverySpanCapture = ArgumentCaptor.forClass(Span.class);
        ArgumentCaptor<Scope> deliveryScopeCapture = ArgumentCaptor.forClass(Scope.class);
        Mockito.verify(message).getTracingAnnotation(Mockito.eq(ANNOTATION_KEY));
        Mockito.verify(message).setTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY), deliverySpanCapture.capture());
        Mockito.verify(message).setTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY), deliveryScopeCapture.capture());
        Mockito.verifyNoMoreInteractions(message);

        assertNotNull("Expected active span to be present", mockTracer.activeSpan());

        Span deliverySpan = deliverySpanCapture.getValue();
        assertNotNull("expected a span from onMessage operation", deliverySpan);
        assertEquals("Unexpected span class", MockSpan.class, deliverySpan.getClass());
        MockSpan deliveryMockSpan = (MockSpan) deliverySpan;

        assertEquals("Unexpected span operation name", ONMESSAGE_SPAN_NAME, deliveryMockSpan.operationName());

        Scope deliveryScope = deliveryScopeCapture.getValue();
        assertNotNull("expected a scope from onMessage operation", deliveryScope);
        assertEquals("Unexpected scope class", ThreadLocalScope.class, deliveryScope.getClass());

        Mockito.when(message.getTracingContext(DELIVERY_SPAN_CONTEXT_KEY)).thenReturn(deliverySpan);
        Mockito.when(message.removeTracingContext(ONMESSAGE_SCOPE_CONTEXT_KEY)).thenReturn(deliveryScope);

        assertTrue("Expected no finished spans", mockTracer.finishedSpans().isEmpty());

        // Do the onMessage completion operation
        jmsTracer.asyncDeliveryComplete(message, DeliveryOutcome.DELIVERED, null);

        Mockito.verify(message).getTracingContext(Mockito.eq(DELIVERY_SPAN_CONTEXT_KEY));
        Mockito.verify(message).removeTracingContext(Mockito.eq(ONMESSAGE_SCOPE_CONTEXT_KEY));
        Mockito.verifyNoMoreInteractions(message);

        List<MockSpan> finishedSpans = mockTracer.finishedSpans();
        assertEquals("Expected 1 finished span: " + finishedSpans, 1, finishedSpans.size());
        assertEquals("Unexpected finished span", deliverySpan, finishedSpans.get(0));

        assertEquals("Expected span to have no parent span", 0, deliveryMockSpan.parentId());

        // Verify tags set on the span
        Map<String, Object> spanTags = deliveryMockSpan.tags();
        assertFalse("Expected some tags", spanTags.isEmpty());
        assertEquals(Tags.SPAN_KIND_CONSUMER, spanTags.get(Tags.SPAN_KIND.getKey()));
        assertEquals(consumerDestination, spanTags.get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
        assertEquals(COMPONENT, spanTags.get(Tags.COMPONENT.getKey()));
    }
}
