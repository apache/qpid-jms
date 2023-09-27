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
package org.apache.qpid.jms.tracing;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;

import org.junit.jupiter.api.Test;

public class JmsNoopTracerFactoryTest {

    @Test
    public void testCreate() {
        JmsTracer tracer  = JmsNoOpTracerFactory.create();

        assertSame(JmsNoOpTracer.INSTANCE, tracer, "Unexpected tracer instance");
    }

    @Test
    public void testCreateURIAndTypeName() throws Exception {
        JmsTracer tracer  = JmsNoOpTracerFactory.create(new URI("amqp://localhost:1234"), JmsNoOpTracerFactory.TYPE_NAME);

        assertSame(JmsNoOpTracer.INSTANCE, tracer, "Unexpected tracer instance");
    }

    @Test
    public void testCreateURIAndTypeNameUnknown() throws Exception {
        try {
            JmsNoOpTracerFactory.create(new URI("amqp://localhost:1234"), "unknown");
            fail("Exception was not thrown");
        } catch (Exception e) {
            // Expected
        }
    }
}
