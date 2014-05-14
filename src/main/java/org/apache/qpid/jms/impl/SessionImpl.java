/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.impl;

import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpReceiver;
import org.apache.qpid.jms.engine.AmqpSender;
import org.apache.qpid.jms.engine.AmqpSession;

public class SessionImpl implements Session
{
    private static final int INITIAL_RECEIVER_CREDIT = 1;

    private final int _acknowledgeMode;
    private final AmqpSession _amqpSession;
    private final ConnectionImpl _connectionImpl;
    private final DestinationHelper _destinationHelper;
    private final MessageIdHelper _messageIdHelper;

    public SessionImpl(int acknowledgeMode, AmqpSession amqpSession, ConnectionImpl connectionImpl, DestinationHelper destinationHelper, MessageIdHelper messageIdHelper)
    {
        _acknowledgeMode = acknowledgeMode;
        _amqpSession = amqpSession;
        _connectionImpl = connectionImpl;
        _destinationHelper = destinationHelper;
        _messageIdHelper = messageIdHelper;
    }

     void establish() throws JmsTimeoutException, JmsInterruptedException
    {
        _connectionImpl.waitUntil(new SimplePredicate("Session established", _amqpSession)
        {
            @Override
            public boolean test()
            {
                return _amqpSession.isEstablished();
            }
        }, AmqpConnection.TIMEOUT);
    }

    ConnectionImpl getConnectionImpl()
    {
        return _connectionImpl;
    }

    private SenderImpl createSender(String address, Destination destination) throws JMSException
    {
        _connectionImpl.lock();
        try
        {
            AmqpSender amqpSender = _amqpSession.createAmqpSender(address);
            SenderImpl sender = new SenderImpl(this, _connectionImpl, amqpSender, destination);
            _connectionImpl.stateChanged();
            sender.establish();
            return sender;
        }
        finally
        {
            _connectionImpl.releaseLock();
        }
    }

    private ReceiverImpl createReceiver(String address, Destination recieverDestination) throws JMSException
    {
        _connectionImpl.lock();
        try
        {
            AmqpReceiver amqpReceiver = _amqpSession.createAmqpReceiver(address);
            ReceiverImpl receiver = new ReceiverImpl(_connectionImpl, this, amqpReceiver, recieverDestination);
            _connectionImpl.stateChanged();
            receiver.establish();

            if(_connectionImpl.isStarted())
            {
                //Issue initial flow for the consumer.
                //TODO: decide on prefetch behaviour, i.e. whether we defer flow or do it now, and what value to use.
                amqpReceiver.credit(INITIAL_RECEIVER_CREDIT);
                _connectionImpl.stateChanged();
            }

            return receiver;
        }
        finally
        {
            _connectionImpl.releaseLock();
        }
    }

    public MessageIdHelper getMessageIdHelper()
    {
        return _messageIdHelper;
    }

    DestinationHelper getDestinationHelper()
    {
        return _destinationHelper;
    }


    //======= JMS Methods =======


    @Override
    public void close() throws JMSException
    {
        _connectionImpl.lock();
        try
        {
            _amqpSession.close();
            _connectionImpl.stateChanged();
            while(!_amqpSession.isClosed())
            {
                _connectionImpl.waitUntil(new SimplePredicate("Session is closed", _amqpSession)
                {
                    @Override
                    public boolean test()
                    {
                        return _amqpSession.isClosed();
                    }
                }, AmqpConnection.TIMEOUT);
            }

            if(_amqpSession.getSessionError().getCondition() != null)
            {
                throw new ConnectionException("Session close failed: " + _amqpSession.getSessionError());
            }
        }
        finally
        {
            _connectionImpl.releaseLock();
        }
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException
    {
        //TODO: use destination helper to perform conversion, extract address etc.
        if(destination == null)
        {
            throw new UnsupportedOperationException("Unspecified destinations are not yet supported");
        }
        else if (destination instanceof Queue)
        {
            Queue queue = (Queue) destination;

            return createSender(queue.getQueueName(), destination);
        }
        else if(destination instanceof Topic)
        {
            throw new UnsupportedOperationException("Topics are not yet supported");
        }
        else
        {
            throw new IllegalArgumentException("Destination expected to be a Queue or a Topic but was: " + destination.getClass());
        }

    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException
    {
        if(destination == null)
        {
            throw new InvalidDestinationException("Null destination provided");
        }
        else if (destination instanceof Queue)
        {
            Queue queue = (Queue) destination;
            return createReceiver(queue.getQueueName(), destination);
        }
        else if(destination instanceof Topic)
        {
            //TODO: support Topic destinations
            throw new UnsupportedOperationException("Topics are not yet supported");
        }
        else
        {
            throw new IllegalArgumentException("Destination expected to be a Queue or a Topic but was: " + destination.getClass());
        }
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException
    {
        return new BytesMessageImpl(this, getConnectionImpl());
    }

    @Override
    public MapMessage createMapMessage() throws JMSException
    {
        return new MapMessageImpl(this, getConnectionImpl());
    }

    @Override
    public Message createMessage() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException
    {
        return new ObjectMessageImpl(this, getConnectionImpl());
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public TextMessage createTextMessage() throws JMSException
    {
        return createTextMessage(null);
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException
    {
        TextMessage msg = new TextMessageImpl(this, _connectionImpl);
        if(text != null)
        {
            msg.setText(text);
        }

        return msg;
    }

    @Override
    public boolean getTransacted() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public int getAcknowledgeMode() throws JMSException
    {
        // TODO do we need to throw an exceptions (e.g. if closed)?
        return _acknowledgeMode;
    }

    @Override
    public void commit() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void rollback() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void recover() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public MessageListener getMessageListener() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void run()
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean NoLocal) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException
    {
        return _destinationHelper.createQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void unsubscribe(String name) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }
}
