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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;

public class JmsContext implements JMSContext, AutoCloseable {

    private final JmsConnection connection;
    private final AtomicLong connectionRefCount;
    private final int sessionMode;

    private JmsSession session;
    private JmsMessageProducer sharedProducer;
    private boolean autoStart = true;

    public JmsContext(JmsConnection connection, int sessionMode) {
        this(connection, sessionMode, new AtomicLong(1));
    }

    private JmsContext(JmsConnection connection, int sessionMode, AtomicLong connectionRefCount) {
        this.connection = connection;
        this.sessionMode = sessionMode;
        this.connectionRefCount = connectionRefCount;
    }

    @Override
    public void start() {
        try {
            connection.start();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public void stop() {
        try {
            connection.stop();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public synchronized void close() {
        JMSRuntimeException failure = null;

        try {
            if (session != null) {
                session.close();
            }
        } catch (JMSException jmse) {
            failure = JmsExceptionSupport.createRuntimeException(jmse);
        }

        if (connectionRefCount.decrementAndGet() == 0) {
            try {
                connection.close();
            } catch (JMSException jmse) {
                if (failure == null) {
                    failure = JmsExceptionSupport.createRuntimeException(jmse);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    //----- Session state management -----------------------------------------//

    @Override
    public void acknowledge() {
        if (getSessionMode() == Session.CLIENT_ACKNOWLEDGE) {
            try {
                getSession().acknowledge(ACK_TYPE.ACCEPTED);
            } catch (JMSException jmse) {
                throw JmsExceptionSupport.createRuntimeException(jmse);
            }
        }
    }

    @Override
    public void commit() {
        try {
            getSession().commit();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public void rollback() {
        try {
            getSession().rollback();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public void recover() {
        try {
            getSession().recover();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public void unsubscribe(String name) {
        try {
            getSession().unsubscribe(name);
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    //----- Message Factory methods ------------------------------------------//

    @Override
    public BytesMessage createBytesMessage() {
        try {
            return getSession().createBytesMessage();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public MapMessage createMapMessage() {
        try {
            return getSession().createMapMessage();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public Message createMessage() {
        try {
            return getSession().createMessage();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public ObjectMessage createObjectMessage() {
        try {
            return getSession().createObjectMessage();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        try {
            return getSession().createObjectMessage(object);
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public StreamMessage createStreamMessage() {
        try {
            return getSession().createStreamMessage();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        try {
            return getSession().createTextMessage();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public TextMessage createTextMessage(String text) {
        try {
            return getSession().createTextMessage(text);
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    //----- Destination Creation ---------------------------------------------//

    @Override
    public Queue createQueue(String queueName) {
        try {
            return getSession().createQueue(queueName);
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public Topic createTopic(String topicName) {
        try {
            return getSession().createTopic(topicName);
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        try {
            return getSession().createTemporaryQueue();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        try {
            return getSession().createTemporaryTopic();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    //----- JMSContext factory methods --------------------------------------//

    @Override
    public synchronized JMSContext createContext(int sessionMode) {
        if (connectionRefCount.get() == 0) {
            throw new IllegalStateRuntimeException("The Connection is closed");
        }

        connectionRefCount.incrementAndGet();

        return new JmsContext(connection, sessionMode, connectionRefCount);
    }

    //----- JMSProducer factory methods --------------------------------------//

    @Override
    public JMSProducer createProducer() {
        try {
            if (sharedProducer == null) {
                synchronized (this) {
                    if (sharedProducer == null) {
                        sharedProducer = (JmsMessageProducer) getSession().createProducer(null);
                    }
                }
            }

            return new JmsProducer(getSession(), sharedProducer);
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    //----- JMSConsumer factory methods --------------------------------------//

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createConsumer(destination)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String selector) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createConsumer(destination, selector)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String selector, boolean noLocal) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createConsumer(destination, selector, noLocal)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createDurableConsumer(topic, name)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String selector, boolean noLocal) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createDurableConsumer(topic, name, selector, noLocal)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String name) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createSharedConsumer(topic, name)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String name, String selector) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createSharedConsumer(topic, name, selector)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createSharedDurableConsumer(topic, name)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String selector) {
        try {
            return startIfNeeded(new JmsConsumer(getSession(), (JmsMessageConsumer) getSession().createSharedDurableConsumer(topic, name, selector)));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    //----- QueueBrowser Factory Methods -------------------------------------//

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        try {
            return startIfNeeded(getSession().createBrowser(queue));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String selector) {
        try {
            return startIfNeeded(getSession().createBrowser(queue, selector));
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    //----- Get or Set Context and Session values ----------------------------//

    @Override
    public boolean getAutoStart() {
        return autoStart;
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    @Override
    public String getClientID() {
        try {
            return connection.getClientID();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public void setClientID(String clientID) {
        try {
            connection.setClientID(clientID);
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public ExceptionListener getExceptionListener() {
        try {
            return connection.getExceptionListener();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        try {
            connection.setExceptionListener(listener);
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public ConnectionMetaData getMetaData() {
        try {
            return connection.getMetaData();
        } catch (JMSException jmse) {
            throw JmsExceptionSupport.createRuntimeException(jmse);
        }
    }

    @Override
    public int getSessionMode() {
        return sessionMode;
    }

    @Override
    public boolean getTransacted() {
        return sessionMode == JMSContext.SESSION_TRANSACTED;
    }

    //----- Internal implementation methods ----------------------------------//

    private JmsSession getSession() {
        if (session == null) {
            synchronized (this) {
                if (session == null) {
                    try {
                        session = (JmsSession) connection.createSession(getSessionMode());
                    } catch (JMSException jmse) {
                        throw JmsExceptionSupport.createRuntimeException(jmse);
                    }
                }
            }
        }

        return session;
    }

    private QueueBrowser startIfNeeded(QueueBrowser browser) throws JMSException {
        if (getAutoStart()) {
            connection.start();
        }

        return browser;
    }

    private JmsConsumer startIfNeeded(JmsConsumer consumer) throws JMSException {
        if (getAutoStart()) {
            connection.start();
        }

        return consumer;
    }
}
