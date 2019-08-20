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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.jms.tracing.TraceableMessage;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;

public class OpenTracingTracer implements JmsTracer {
    static final String REDELIVERIES_EXCEEDED = "redeliveries-exceeded";
    static final String MESSAGE_EXPIRED = "message-expired";
    static final String SEND_SPAN_NAME = "amqp-delivery-send";
    static final String RECEIVE_SPAN_NAME = "receive";
    static final String ONMESSAGE_SPAN_NAME = "onMessage";
    static final String DELIVERY_SETTLED = "delivery settled";
    static final String STATE = "state";
    static final String COMPONENT = "qpid-jms";
    static final Object ERROR_EVENT = "error";

    static final String SEND_SPAN_CONTEXT_KEY = "sendSpan";
    static final String ARRIVING_SPAN_CTX_CONTEXT_KEY = "arrivingContext";
    static final String DELIVERY_SPAN_CONTEXT_KEY = "deliverySpan";
    static final String ONMESSAGE_SCOPE_CONTEXT_KEY = "onMessageScope";

    static final String ANNOTATION_KEY = "x-opt-qpid-tracestate";

    private Tracer tracer;
    private boolean closeUnderlyingTracer;

    OpenTracingTracer(Tracer tracer, boolean closeUnderlyingTracer) {
        this.tracer = tracer;
        this.closeUnderlyingTracer = closeUnderlyingTracer;
    }

    @Override
    public void initSend(TraceableMessage message, String address) {
        Span span = tracer.buildSpan(SEND_SPAN_NAME)
                          .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_PRODUCER)
                          .withTag(Tags.MESSAGE_BUS_DESTINATION, address)
                          .withTag(Tags.COMPONENT, COMPONENT)
                          .start();

        LazyTextMapInject carrier = new LazyTextMapInject();

        tracer.inject(span.context(), Format.Builtin.TEXT_MAP, carrier);

        if(carrier.getInjectMap() != null) {
            message.setTracingAnnotation(ANNOTATION_KEY, carrier.getInjectMap());
        } else {
            message.removeTracingAnnotation(ANNOTATION_KEY);
        }

        message.setTracingContext(SEND_SPAN_CONTEXT_KEY, span);
    }

    @Override
    public void completeSend(TraceableMessage message, String outcome) {
        Object cachedSpan = message.getTracingContext(SEND_SPAN_CONTEXT_KEY);
        if (cachedSpan != null) {
            Span span = (Span) cachedSpan;

            Map<String, String> fields = new HashMap<>();
            fields.put(Fields.EVENT, DELIVERY_SETTLED);
            fields.put(STATE, outcome == null ? "null" : outcome);

            span.log(fields);

            span.finish();
        }
    }

    private SpanContext extract(TraceableMessage message) {
        SpanContext spanContext = null;

        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) message.getTracingAnnotation(ANNOTATION_KEY);
        if(headers != null && !headers.isEmpty()) {
            spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(headers));
        }

        if(spanContext != null) {
            message.setTracingContext(ARRIVING_SPAN_CTX_CONTEXT_KEY, spanContext);
        }

        return spanContext;
    }

    @Override
    public void syncReceive(TraceableMessage message, String address, DeliveryOutcome outcome) {
        SpanContext context = extract(message);

        Span span = tracer.buildSpan(RECEIVE_SPAN_NAME)
                          .asChildOf(context)
                          .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER)
                          .withTag(Tags.MESSAGE_BUS_DESTINATION, address)
                          .withTag(Tags.COMPONENT, COMPONENT)
                          .start();
        try {
            addDeliveryLogIfNeeded(outcome, span);
        } finally {
            span.finish();
        }

        message.setTracingContext(DELIVERY_SPAN_CONTEXT_KEY, span);
    }

    private void addDeliveryLogIfNeeded(DeliveryOutcome outcome, Span span) {
        Map<String, Object> fields = null;
        if (outcome == DeliveryOutcome.EXPIRED) {
            fields = new HashMap<>();
            fields.put(Fields.EVENT, MESSAGE_EXPIRED);
        } else if (outcome == DeliveryOutcome.REDELIVERIES_EXCEEDED) {
            fields = new HashMap<>();
            fields.put(Fields.EVENT, REDELIVERIES_EXCEEDED);
        }

        if (fields != null) {
            span.log(fields);
        }
    }

    @Override
    public void asyncDeliveryInit(TraceableMessage message, String address) {
        SpanContext context = extract(message);

        Span span = tracer.buildSpan(ONMESSAGE_SPAN_NAME)
                          .ignoreActiveSpan()
                          .asChildOf(context)
                          .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER)
                          .withTag(Tags.MESSAGE_BUS_DESTINATION, address)
                          .withTag(Tags.COMPONENT, COMPONENT)
                          .start();

        message.setTracingContext(DELIVERY_SPAN_CONTEXT_KEY, span);

        Scope scope = tracer.activateSpan(span);
        message.setTracingContext(ONMESSAGE_SCOPE_CONTEXT_KEY, scope);
    }

    @Override
    public void asyncDeliveryComplete(TraceableMessage message, DeliveryOutcome outcome, Throwable throwable) {
        Scope scope = (Scope) message.removeTracingContext(ONMESSAGE_SCOPE_CONTEXT_KEY);
        try {
            if (scope != null) {
                scope.close();
            }
        } finally {
            Span span = (Span) message.getTracingContext(DELIVERY_SPAN_CONTEXT_KEY);
            if (span != null) {
                try {
                    if (throwable != null) {
                        span.setTag(Tags.ERROR, true);

                        Map<String, Object> fields = new HashMap<>();
                        fields.put(Fields.EVENT, ERROR_EVENT);
                        fields.put(Fields.ERROR_OBJECT, throwable);
                        fields.put(Fields.MESSAGE, "Application error, exception thrown from onMessage.");

                        span.log(fields);
                    } else {
                        addDeliveryLogIfNeeded(outcome, span);
                    }
                } finally {
                    span.finish();
                }
            }
        }
    }

    @Override
    public void close() {
        if (closeUnderlyingTracer) {
            tracer.close();
        }
    }

    private static class LazyTextMapInject implements TextMap {
        private Map<String,String> injectMap = null;

        @Override
        public void put(String key, String value) {
            if(injectMap == null) {
                injectMap = new HashMap<>();
            }

            injectMap.put(key, value);
        }

        @Override
        public Iterator<Entry<String, String>> iterator() {
            throw new UnsupportedOperationException();
        }

        Map<String, String> getInjectMap() {
            return injectMap;
        }
    }
}
