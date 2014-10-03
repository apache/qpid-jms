/**
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
import org.apache.qpid.jms.message.facade.JmsObjectMessageFacade;
import org.apache.qpid.jms.message.facade.JmsTextMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;

/**
 * AMQP Message Factory instance used to create new JmsMessage types that wrap an
 * Proton AMQP Message.  This class is used by the JMS layer to create its JMS
 * Message instances, the messages returned here should be created in a proper
 * initially empty state for the client to populate.
 */
public class AmqpJmsMessageFactory implements JmsMessageFactory {

    private AmqpConnection connection;

    public AmqpJmsMessageFactory() {
    }

    public AmqpJmsMessageFactory(AmqpConnection connection) {
        this.connection = connection;
    }

    public AmqpConnection getAmqpConnection() {
        return this.connection;
    }

    public void setAmqpConnection(AmqpConnection connection) {
        this.connection = connection;
    }

    @Override
    public JmsMessage createMessage() throws JMSException {
        return new JmsMessage(new AmqpJmsMessageFacade(connection));
    }

    @Override
    public JmsTextMessage createTextMessage() throws JMSException {
        return createTextMessage(null);
    }

    @Override
    public JmsTextMessage createTextMessage(String payload) throws JMSException {

        JmsTextMessageFacade facade = new AmqpJmsTextMessageFacade(connection);

        if (payload != null) {
            facade.setText(payload);
        }

        return new JmsTextMessage(facade);
    }

    @Override
    public JmsBytesMessage createBytesMessage() throws JMSException {
        return new JmsBytesMessage(new AmqpJmsBytesMessageFacade(connection));
    }

    @Override
    public JmsMapMessage createMapMessage() throws JMSException {
        return new JmsMapMessage(new AmqpJmsMapMessageFacade(connection));
    }

    @Override
    public JmsStreamMessage createStreamMessage() throws JMSException {
        return new JmsStreamMessage(new AmqpJmsStreamMessageFacade(connection));
    }

    @Override
    public JmsObjectMessage createObjectMessage() throws JMSException {
        return createObjectMessage(null);
    }

    @Override
    public JmsObjectMessage createObjectMessage(Serializable payload) throws JMSException {
        // TODO Implement [Connection?] configuration to control default delegate type?
        JmsObjectMessageFacade facade = new AmqpJmsObjectMessageFacade(connection, false);

        if (payload != null) {
            try {
                facade.setObject(payload);
            } catch (IOException e) {
                throw JmsExceptionSupport.create(e);
            }
        }

        return new JmsObjectMessage(facade);
    }
}
