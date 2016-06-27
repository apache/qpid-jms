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
package org.apache.qpid.jms.policy;

import javax.jms.ObjectMessage;

import org.apache.qpid.jms.JmsDestination;

/**
 * Defines the interface for a policy object that controls what types of message
 * content are permissible when the body of an incoming ObjectMessage is being
 * deserialized.
 */
public interface JmsDeserializationPolicy {

    JmsDeserializationPolicy copy();

    /**
     * Returns whether the given class is a trusted type and can be deserialized
     * by the client when calls to {@link ObjectMessage#getObject()} are made.
     *
     * @param destination
     *      the Destination for the message containing the type to be deserialized.
     * @param clazz
     *      the Type of the object that is about to be read.
     *
     * @return true if the type is trusted or false if not.
     */
    boolean isTrustedType(JmsDestination destination, Class<?> clazz);

}
