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

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.message.JmsMessageIDBuilder;

/**
 * The default MessageID policy used for all MessageProducers created from the
 * client's connection factory.
 */
public class JmsDefaultMessageIDPolicy implements JmsMessageIDPolicy {

    private JmsMessageIDBuilder messageIDBuilder = JmsMessageIDBuilder.BUILTIN.DEFAULT.createBuilder();

    /**
     * Initialize default Message ID builder policy
     */
    public JmsDefaultMessageIDPolicy() {
    }

    /**
     * Creates a new JmsDefaultMessageIDPolicy instance copied from the source policy.
     *
     * @param source
     *      The policy instance to copy values from.
     */
    public JmsDefaultMessageIDPolicy(JmsDefaultMessageIDPolicy source) {
        this.messageIDBuilder = source.messageIDBuilder;
    }

    @Override
    public JmsDefaultMessageIDPolicy copy() {
        return new JmsDefaultMessageIDPolicy(this);
    }

    @Override
    public JmsMessageIDBuilder getMessageIDBuilder(JmsSession session, JmsDestination destination) {
        return messageIDBuilder;
    }

    /**
     * Sets the type of the Message IDs used to populate the outgoing Messages
     *
     * @param type
     *      The name of the Message type to use when sending a message.
     */
    public void setMessageIDType(String type) {
        this.messageIDBuilder = JmsMessageIDBuilder.BUILTIN.create(type);
    }

    /**
     * @return the type name of the configured JmsMessageIDBuilder.
     */
    public String getMessageIDType() {
        return this.messageIDBuilder.toString();
    }

    public JmsMessageIDBuilder getMessageIDBuilder() {
        return messageIDBuilder;
    }

    public void setMessageIDBuilder(JmsMessageIDBuilder messageIDBuilder) {
        this.messageIDBuilder = messageIDBuilder;
    }
}
