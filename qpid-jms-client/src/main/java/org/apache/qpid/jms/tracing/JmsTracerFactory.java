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

import java.io.IOException;
import java.net.URI;

import org.apache.qpid.jms.util.FactoryFinder;

public abstract class JmsTracerFactory {

    private static final FactoryFinder<JmsTracerFactory> TRACER_FACTORY_FINDER =
        new FactoryFinder<JmsTracerFactory>(JmsTracerFactory.class,
            "META-INF/services/" + JmsTracerFactory.class.getPackage().getName().replace(".", "/") + "/");

    public abstract JmsTracer createTracer(URI remoteURI, String name) throws Exception;

    /**
     * Creates a JmsTracer using factory with the given name and any relevant configuration
     * properties set on the given remote URI.
     *
     * @param remoteURI
     *        The connection uri.
     * @param name
     *        The name that describes the desired tracer factory.
     * @return a tracer instance matching the name.
     *
     * @throws Exception if an error occurs while creating the tracer.
     */
    public static JmsTracer create(URI remoteURI, String name) throws Exception {
        JmsTracerFactory factory = findTracerFactory(name);

        return factory.createTracer(remoteURI, name);
    }

    /**
     * Searches for a JmsTracerFactory by using the given name.
     *
     * The search first checks the local cache of factories before moving on to search in the class path.
     *
     * @param name
     *        The name that describes the desired tracer factory.
     *
     * @return a tracer factory instance matching the name.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static JmsTracerFactory findTracerFactory(String name) throws IOException {
        if (name == null || name.isEmpty()) {
            throw new IOException("No Tracer name specified.");
        }

        JmsTracerFactory factory = null;
        try {
            factory = TRACER_FACTORY_FINDER.newInstance(name);
        } catch (Throwable e) {
            throw new IOException("Tracer name NOT recognized: [" + name + "]", e);
        }

        return factory;
    }
}
