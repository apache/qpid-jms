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
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;

import org.apache.qpid.jms.meta.JmsProducerId;

/**
 * Implementation of a Queue Sender
 */
public class JmsQueueSender extends JmsMessageProducer implements AutoCloseable, QueueSender {

    protected JmsQueueSender(JmsProducerId id, JmsSession session, JmsDestination destination) throws JMSException {
        super(id, session, destination);
    }

    /**
     * @see javax.jms.QueueSender#getQueue()
     */
    @Override
    public Queue getQueue() throws IllegalStateException {
        checkClosed();
        return (Queue) this.producerInfo.getDestination();
    }

    /**
     * @see javax.jms.QueueSender#send(javax.jms.Queue, javax.jms.Message)
     */
    @Override
    public void send(Queue queue, Message message) throws JMSException {
        super.send(queue, message);
    }

    /**
     * @see javax.jms.QueueSender#send(javax.jms.Queue, javax.jms.Message, int, int, long)
     */
    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        super.send(queue, message, deliveryMode, priority, timeToLive);
    }
}
