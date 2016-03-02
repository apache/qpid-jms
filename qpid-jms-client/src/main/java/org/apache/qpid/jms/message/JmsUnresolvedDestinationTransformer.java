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
package org.apache.qpid.jms.message;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;

/**
 * Defines an interface for a handler object that will be called when the
 * transformation of a JMS Destination object fails to determine the proper
 * destination type to create.
 */
public interface JmsUnresolvedDestinationTransformer {

    /**
     * Given a JMS Destination attempt to determine the type of JmsDestination to
     * create.
     *
     * @param destination
     *        the JMS destination that requires conversion to a JmsDestination type.
     *
     * @return a new JmsDestination instance to match the foreign destination.
     *
     * @throws JMSException if an error occurs during the transformation.
     */
    public JmsDestination transform(Destination destination) throws JMSException;

    /**
     * Given a destination name return a matching JmsDestination object.
     *
     * @param destination
     *        the name of the destination to create a JmsDestination type for.
     *
     * @return a new JmsDestination object that matches the given name.
     *
     * @throws JMSException if an error occurs while transforming the name.
     */
    public JmsDestination transform(String destination) throws JMSException;

}
