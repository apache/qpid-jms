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

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.meta.JmsConnectionId;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.mockito.Mockito;

public class AmqpJmsMessageTypesTestCase extends QpidJmsTestCase {

    private JmsDestination consumerDestination;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        consumerDestination = new JmsTopic("TestTopic");
    };

    //---------- Test Support Methods ----------------------------------------//

    protected AmqpJmsMessageFacade createNewMessageFacade() {
        AmqpJmsMessageFacade facade = new AmqpJmsMessageFacade();
        facade.initialize(createMockAmqpConnection());
        return facade;
    }

    protected AmqpJmsMessageFacade createReceivedMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        AmqpJmsMessageFacade facade = new AmqpJmsMessageFacade();
        initializeReceivedMessage(facade, amqpConsumer, message);
        return facade;
    }

    protected AmqpJmsTextMessageFacade createNewTextMessageFacade() {
        AmqpJmsTextMessageFacade facade = new AmqpJmsTextMessageFacade();
        facade.initialize(createMockAmqpConnection());
        return facade;
    }

    protected AmqpJmsTextMessageFacade createReceivedTextMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        AmqpJmsTextMessageFacade facade = new AmqpJmsTextMessageFacade();
        initializeReceivedMessage(facade, amqpConsumer, message);
        return facade;
    }

    protected AmqpJmsBytesMessageFacade createNewBytesMessageFacade() {
        AmqpJmsBytesMessageFacade facade = new AmqpJmsBytesMessageFacade();
        facade.initialize(createMockAmqpConnection());
        return facade;
    }

    protected AmqpJmsBytesMessageFacade createReceivedBytesMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        AmqpJmsBytesMessageFacade facade = new AmqpJmsBytesMessageFacade();
        initializeReceivedMessage(facade, amqpConsumer, message);
        return facade;
    }

    protected AmqpJmsMapMessageFacade createNewMapMessageFacade() {
        AmqpJmsMapMessageFacade facade = new AmqpJmsMapMessageFacade();
        facade.initialize(createMockAmqpConnection());
        return facade;
    }

    protected AmqpJmsMapMessageFacade createReceivedMapMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        AmqpJmsMapMessageFacade facade = new AmqpJmsMapMessageFacade();
        initializeReceivedMessage(facade, amqpConsumer, message);
        return facade;
    }

    protected AmqpJmsStreamMessageFacade createNewStreamMessageFacade() {
        AmqpJmsStreamMessageFacade facade = new AmqpJmsStreamMessageFacade();
        facade.initialize(createMockAmqpConnection());
        return facade;
    }

    protected AmqpJmsStreamMessageFacade createReceivedStreamMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        AmqpJmsStreamMessageFacade facade = new AmqpJmsStreamMessageFacade();
        initializeReceivedMessage(facade, amqpConsumer, message);
        return facade;
    }

    protected AmqpJmsObjectMessageFacade createNewObjectMessageFacade(boolean amqpTyped) {
        AmqpJmsObjectMessageFacade facade = new AmqpJmsObjectMessageFacade();
        facade.initialize(createMockAmqpConnection(amqpTyped));
        return facade;
    }

    protected AmqpJmsObjectMessageFacade createReceivedObjectMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        AmqpJmsObjectMessageFacade facade = new AmqpJmsObjectMessageFacade();
        initializeReceivedMessage(facade, amqpConsumer, message);
        return facade;
    }

    protected void initializeReceivedMessage(AmqpJmsMessageFacade facade, AmqpConsumer amqpConsumer, Message message) {
        facade.setHeader(message.getHeader());
        facade.setDeliveryAnnotations(message.getDeliveryAnnotations());
        facade.setMessageAnnotations(message.getMessageAnnotations());
        facade.setProperties(message.getProperties());
        facade.setApplicationProperties(message.getApplicationProperties());
        facade.setBody(message.getBody());
        facade.setFooter(message.getFooter());
        facade.initialize(amqpConsumer);
    }

    protected AmqpConsumer createMockAmqpConsumer() {
        JmsConsumerId consumerId = new JmsConsumerId("ID:MOCK:1:1:1");
        AmqpConnection connection = createMockAmqpConnection();
        AmqpConsumer consumer = Mockito.mock(AmqpConsumer.class);
        Mockito.when(consumer.getConnection()).thenReturn(connection);
        Mockito.when(consumer.getDestination()).thenReturn(consumerDestination);
        Mockito.when(consumer.getResourceInfo()).thenReturn(new JmsConsumerInfo(consumerId, null));
        return consumer;
    }

    protected AmqpConnection createMockAmqpConnection() {
        return createMockAmqpConnection(false);
    }

    protected AmqpConnection createMockAmqpConnection(boolean amqpTyped) {
        JmsConnectionId connectionId = new JmsConnectionId("ID:MOCK:1");
        AmqpConnection connection = Mockito.mock(AmqpConnection.class);
        Mockito.when(connection.getResourceInfo()).thenReturn(new JmsConnectionInfo(connectionId));
        Mockito.when(connection.isObjectMessageUsesAmqpTypes()).thenReturn(amqpTyped);

        return connection;
    }
}
