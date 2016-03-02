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
package org.apache.qpid.jms;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

import org.apache.qpid.jms.meta.JmsConsumerId;

/**
 * Implementation of a JMS QueueReceiver
 */
public class JmsQueueReceiver extends JmsMessageConsumer implements AutoCloseable, QueueReceiver {

    /**
     * Constructor
     *
     * @param id
     *      This receiver's assigned Id.
     * @param session
     *      The session that created this receiver.
     * @param dest
     *      The destination that this receiver listens on.
     * @param selector
     *      The selector used to filter messages for this receiver.
     *
     * @throws JMSException if an error occurs during the creation of the QueueReceiver.
     */
    protected JmsQueueReceiver(JmsConsumerId id, JmsSession session, JmsDestination dest, String selector) throws JMSException {
        super(id, session, dest, selector, false);
    }

    /**
     * @see javax.jms.QueueReceiver#getQueue()
     */
    @Override
    public Queue getQueue() throws IllegalStateException {
        checkClosed();
        return (Queue) this.getDestination();
    }
}
