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
package org.apache.qpid.jms.provider.amqp.message;

import java.io.IOException;
import java.io.Serializable;

import javax.jms.JMSException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsMapMessage;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.JmsStreamMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;

/**
 * AMQP Message Factory instance used to create new JmsMessage types that wrap an
 * Proton AMQP Message.  This class is used by the JMS layer to create its JMS
 * Message instances, the messages returned here should be created in a proper
 * initially empty state for the client to populate.
 */
public class AmqpJmsMessageFactory implements JmsMessageFactory {

    private final AmqpConnection connection;

    public AmqpJmsMessageFactory(AmqpConnection connection) {
        this.connection = connection;
    }

    public AmqpConnection getAmqpConnection() {
        return this.connection;
    }

    @Override
    public JmsMessage createMessage() throws JMSException {
        AmqpJmsMessageFacade facade = new AmqpJmsMessageFacade();
        facade.initialize(connection);
        return facade.asJmsMessage();
    }

    @Override
    public JmsTextMessage createTextMessage() throws JMSException {
        return createTextMessage(null);
    }

    @Override
    public JmsTextMessage createTextMessage(String payload) throws JMSException {
        AmqpJmsTextMessageFacade facade = new AmqpJmsTextMessageFacade();
        facade.initialize(connection);

        if (payload != null) {
            facade.setText(payload);
        }

        return facade.asJmsMessage();
    }

    @Override
    public JmsBytesMessage createBytesMessage() throws JMSException {
        AmqpJmsBytesMessageFacade facade = new AmqpJmsBytesMessageFacade();
        facade.initialize(connection);
        return facade.asJmsMessage();
    }

    @Override
    public JmsMapMessage createMapMessage() throws JMSException {
        AmqpJmsMapMessageFacade facade = new AmqpJmsMapMessageFacade();
        facade.initialize(connection);
        return facade.asJmsMessage();
    }

    @Override
    public JmsStreamMessage createStreamMessage() throws JMSException {
        AmqpJmsStreamMessageFacade facade = new AmqpJmsStreamMessageFacade();
        facade.initialize(connection);
        return facade.asJmsMessage();
    }

    @Override
    public JmsObjectMessage createObjectMessage() throws JMSException {
        return createObjectMessage(null);
    }

    @Override
    public JmsObjectMessage createObjectMessage(Serializable payload) throws JMSException {
        AmqpJmsObjectMessageFacade facade = new AmqpJmsObjectMessageFacade();

        facade.initialize(connection);
        if (payload != null) {
            try {
                facade.setObject(payload);
            } catch (IOException e) {
                throw JmsExceptionSupport.create(e);
            }
        }

        return facade.asJmsMessage();
    }
}
