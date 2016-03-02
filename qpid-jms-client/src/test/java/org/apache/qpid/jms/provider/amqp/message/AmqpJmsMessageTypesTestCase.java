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

import java.nio.charset.StandardCharsets;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsTopic;
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
        return new AmqpJmsMessageFacade(createMockAmqpConnection());
    }

    protected AmqpJmsMessageFacade createReceivedMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsMessageFacade(amqpConsumer, message);
    }

    protected AmqpJmsTextMessageFacade createNewTextMessageFacade() {
        return new AmqpJmsTextMessageFacade(createMockAmqpConnection());
    }

    protected AmqpJmsTextMessageFacade createReceivedTextMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsTextMessageFacade(amqpConsumer, message, StandardCharsets.UTF_8);
    }

    protected AmqpJmsBytesMessageFacade createNewBytesMessageFacade() {
        return new AmqpJmsBytesMessageFacade(createMockAmqpConnection());
    }

    protected AmqpJmsBytesMessageFacade createReceivedBytesMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsBytesMessageFacade(amqpConsumer, message);
    }

    protected AmqpJmsMapMessageFacade createNewMapMessageFacade() {
        return new AmqpJmsMapMessageFacade(createMockAmqpConnection());
    }

    protected AmqpJmsMapMessageFacade createReceivedMapMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsMapMessageFacade(amqpConsumer, message);
    }

    protected AmqpJmsStreamMessageFacade createNewStreamMessageFacade() {
        return new AmqpJmsStreamMessageFacade(createMockAmqpConnection());
    }

    protected AmqpJmsStreamMessageFacade createReceivedStreamMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsStreamMessageFacade(amqpConsumer, message);
    }

    protected AmqpJmsObjectMessageFacade createNewObjectMessageFacade(boolean amqpTyped) {
        return new AmqpJmsObjectMessageFacade(createMockAmqpConnection(), amqpTyped);
    }

    protected AmqpJmsObjectMessageFacade createReceivedObjectMessageFacade(AmqpConsumer amqpConsumer, Message message) {
        return new AmqpJmsObjectMessageFacade(amqpConsumer, message);
    }

    protected AmqpConsumer createMockAmqpConsumer() {
        AmqpConsumer consumer = Mockito.mock(AmqpConsumer.class);
        Mockito.when(consumer.getConnection()).thenReturn(createMockAmqpConnection());
        Mockito.when(consumer.getDestination()).thenReturn(consumerDestination);
        return consumer;
    }

    protected AmqpConnection createMockAmqpConnection() {
        return Mockito.mock(AmqpConnection.class);
    }
}
