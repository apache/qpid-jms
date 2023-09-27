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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;

import org.apache.qpid.jms.tracing.JmsTracer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.opentracing.Tracer;

public class OpenTracingTracerFactoryTest {

    @Test
    public void testCreateWithProvidedTracer() {
        // As used when setting a JmsTracer on the connection factory
        Tracer mock = Mockito.mock(Tracer.class);
        JmsTracer jmsTracer  = OpenTracingTracerFactory.create(mock);

        assertEquals(OpenTracingTracer.class, jmsTracer.getClass(), "Unexpected tracer instance type");

        //Check it doesn't close underlying tracer
        Mockito.verifyNoInteractions(mock);
        jmsTracer.close();
        Mockito.verifyNoInteractions(mock);
    }

    @Test
    public void testCreateWithProvidedTracerCloseProvider() {
        // As used when setting a JmsTracer on the connection factory
        Tracer mock = Mockito.mock(Tracer.class);

        //Check it doesn't close underlying tracer if not asked
        JmsTracer jmsTracerDontClose  = OpenTracingTracerFactory.create(mock, false);
        Mockito.verifyNoInteractions(mock);
        jmsTracerDontClose.close();
        Mockito.verifyNoInteractions(mock);

        //Check it does close underlying tracer when asked
        JmsTracer jmsTracerClose  = OpenTracingTracerFactory.create(mock, true);
        Mockito.verifyNoInteractions(mock);
        jmsTracerClose.close();
        Mockito.verify(mock).close();
        Mockito.verifyNoMoreInteractions(mock);
    }

    @Test
    public void testCreateWithURIAndTypeName() throws Exception {
        // As used when requesting tracing via URI option
        JmsTracer jmsTracer  = OpenTracingTracerFactory.create(new URI("amqp://localhost:1234"), OpenTracingTracerFactory.TYPE_NAME);

        assertEquals(OpenTracingTracer.class, jmsTracer.getClass(), "Unexpected tracer instance type");
    }

    @Test
    public void testCreateWithURIAndTypeNameUnknown() throws Exception {
        try {
            OpenTracingTracerFactory.create(new URI("amqp://localhost:1234"), "unknown");
            fail("Exception was not thrown");
        } catch (Exception e) {
            // Expected
        }
    }
}
