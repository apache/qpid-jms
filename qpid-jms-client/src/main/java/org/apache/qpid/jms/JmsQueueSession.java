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
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.meta.JmsSessionId;

/**
 * JMS QueueSession implementation
 */
public class JmsQueueSession extends JmsSession implements AutoCloseable {

    protected JmsQueueSession(JmsConnection connection, JmsSessionId sessionId, int acknowledgementMode) throws JMSException {
        super(connection, sessionId, acknowledgementMode);
    }

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            throw new IllegalStateException("Operation not supported by a QueueSession");
        }
        return super.createConsumer(destination);
    }

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        if (destination instanceof Topic) {
            throw new IllegalStateException("Operation not supported by a QueueSession");
        }
        return super.createConsumer(destination, messageSelector);
    }

    /**
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String, boolean)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        if (destination instanceof Topic) {
            throw new IllegalStateException("Operation not supported by a QueueSession");
        }
        return super.createConsumer(destination, messageSelector, noLocal);
    }

    /**
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic, java.lang.String)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic, java.lang.String, java.lang.String, boolean)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.Session#createProducer(javax.jms.Destination)
     */
    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            throw new IllegalStateException("Operation not supported by a QueueSession");
        }
        return super.createProducer(destination);
    }

    /**
     * @see javax.jms.Session#createTemporaryTopic()
     */
    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.Session#createTopic(java.lang.String)
     */
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.Session#unsubscribe(java.lang.String)
     */
    @Override
    public void unsubscribe(String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.TopicSession#createPublisher(javax.jms.Topic)
     */
    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.TopicSession#createSubscriber(javax.jms.Topic)
     */
    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.TopicSession#createSubscriber(javax.jms.Topic, java.lang.String, boolean)
     */
    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.Session#createSharedConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.Session#createSharedConsumer(javax.jms.Topic, java.lang.String, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String name, String selector) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.Session#createSharedDurableConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @see javax.jms.Session#createSharedDurableConsumer(javax.jms.Topic, java.lang.String, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String selector) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }
}
