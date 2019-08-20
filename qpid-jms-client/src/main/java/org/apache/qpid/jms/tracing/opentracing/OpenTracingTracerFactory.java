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

import java.net.URI;

import javax.jms.Connection;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.jms.tracing.JmsTracerFactory;

import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

public class OpenTracingTracerFactory extends JmsTracerFactory {

    static final String TYPE_NAME = "opentracing";

    /**
     * Creates a JmsTracer wrapping a provided Open Tracing tracer instance
     * previously created by the application.
     *
     * Used for programmatic creation of JmsTracer to explicitly set on a ConnectionFactory
     * when not using the jms.tracing URI option, or {@link JmsConnectionFactory#setTracing(String)},
     * which both utilise the {@link GlobalTracer}.
     *
     * The returned JmsTracer will no-op when its close method is called during
     * {@link Connection#close()} closure, to allow using the given Tracer with multiple
     * connections and elsewhere in the application. Equivalent to calling
     * {@link #create(Tracer, boolean) #create(Tracer, false)}
     *
     * @param tracer
     *            The Open Tracing tracer to use
     * @return a JmsTracer instance using the provided OpenTracing tracer.
     */
    public static JmsTracer create(Tracer tracer) {
        return create(tracer, false);
    }

    /**
     * As {@link #create(Tracer)}, but providing control over whether the given Tracer
     * has its close method called when the returned JmsTracer is closed during
     * {@link Connection#close()}.
     *
     * @param tracer
     *            The Open Tracing tracer to use
     * @param closeUnderlyingTracer
     *            Whether to close the underlying tracer during {@link Connection#close()}
     * @return a JmsTracer instance using the provided OpenTracing tracer.
     */
    public static JmsTracer create(Tracer tracer, boolean closeUnderlyingTracer) {
        return new OpenTracingTracer(tracer, closeUnderlyingTracer);
    }

    @Override
    public JmsTracer createTracer(URI remoteURI, String name) throws Exception {
        Tracer tracer = GlobalTracer.get();
        return new OpenTracingTracer(tracer, false);
    }
}
