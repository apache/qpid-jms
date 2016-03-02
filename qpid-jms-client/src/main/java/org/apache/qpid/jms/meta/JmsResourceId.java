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
package org.apache.qpid.jms.meta;

/**
 * Base for all Id type classes used in the JMS Framework
 */
public interface JmsResourceId {

    /**
     * Allows a Provider to embed a hint in this Id value for later use. The
     * hint can allow the provider to more easier locate state data for a resource
     *
     * @param hint
     *        The value to add into this Id.
     */
    void setProviderHint(Object hint);

    /**
     * Return the previously stored Provider hint object.
     *
     * @return the previously stored Provider hint object.
     */
    Object getProviderHint();

    /**
     * Allows a Provider to embed a bit of Context for later use.  The context
     * can be some state data needed between asynchronous requests etc.
     *
     * @param value
     *      The value to add as context for this Id.
     */
    void setProviderContext(Object value);

    /**
     * Return the previously stored Provider context object.
     *
     * @return the previously stored Provider context object.
     */
    Object getProviderContext();

}
