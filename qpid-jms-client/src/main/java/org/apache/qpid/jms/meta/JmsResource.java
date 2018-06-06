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
 * Base class for the JMS object representing JMS resources such as Connection, Session, etc.
 */
public interface JmsResource {

    enum ResourceState {
        INITIALIZED,
        OPEN,
        CLOSED,
        REMOTELY_CLOSED;
    }

    /**
     * Returns the assigned resource ID for this JmsResource instance.
     *
     * @return the assigned resource ID for this JmsResource instance.
     */
    JmsResourceId getId();

    /**
     * Allows a visitor object to walk the resources and process them.
     *
     * @param visitor
     *        The visitor instance that is processing this resource.
     *
     * @throws Exception if an error occurs while visiting this resource.
     */
    void visit(JmsResourceVistor visitor) throws Exception;

    /**
     * @return the current state of this resource.
     */
    ResourceState getState();

    /**
     * Sets or updates the current state of this resource.
     *
     * @param state
     * 		The new state to apply to this resource.
     */
    void setState(ResourceState state);

}
