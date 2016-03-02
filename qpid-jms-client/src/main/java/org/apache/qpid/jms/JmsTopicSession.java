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

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.TemporaryQueue;

import org.apache.qpid.jms.meta.JmsSessionId;

/**
 * Implementation of a TopicSession
 */
public class JmsTopicSession extends JmsSession implements AutoCloseable {

    protected JmsTopicSession(JmsConnection connection, JmsSessionId sessionId, int acknowledgementMode) throws JMSException {
        super(connection, sessionId, acknowledgementMode);
    }

    /**
     * @see javax.jms.Session#createBrowser(javax.jms.Queue)
     */
    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @see javax.jms.Session#createBrowser(javax.jms.Queue, java.lang.String)
     */
    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        if (destination instanceof Queue) {
            throw new IllegalStateException("Operation not supported by a TopicSession");
        }
        return super.createConsumer(destination);
    }

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        if (destination instanceof Queue) {
            throw new IllegalStateException("Operation not supported by a TopicSession");
        }
        return super.createConsumer(destination, messageSelector);
    }

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        if (destination instanceof Queue) {
            throw new IllegalStateException("Operation not supported by a TopicSession");
        }
        return super.createConsumer(destination, messageSelector, noLocal);
    }

    /**
     * @see javax.jms.Session#createProducer(javax.jms.Destination)
     */
    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        if (destination instanceof Queue) {
            throw new IllegalStateException("Operation not supported by a TopicSession");
        }
        return super.createProducer(destination);
    }

    /**
     * @see javax.jms.Session#createQueue(java.lang.String)
     */
    @Override
    public Queue createQueue(String queueName) throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @see javax.jms.Session#createTemporaryQueue()
     */
    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @see javax.jms.QueueSession#createReceiver(javax.jms.Queue)
     */
    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @see javax.jms.QueueSession#createReceiver(javax.jms.Queue, java.lang.String)
     */
    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @see javax.jms.QueueSession#createSender(javax.jms.Queue)
     */
    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }
}
