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
package org.apache.qpid.jms;

import java.util.logging.Logger;

import org.apache.qpid.jms.impl.ConnectionImpl;
import org.apache.qpid.jms.impl.ReceivedMessageImpl;
import org.apache.qpid.jms.impl.ReceiverImpl;
import org.apache.qpid.jms.impl.SenderImpl;
import org.apache.qpid.jms.impl.SessionImpl;
import org.apache.qpid.proton.ProtonFactoryLoader;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageFactory;
import org.junit.Test;

/**
 * VERY simple POC for a JMS-like client based on Proton-J
 *
 * Requires an AMQP 1.0 'broker' a localhost:5672 with a
 * node called "queue" to which a message can be sent
 * and received
 */
public class JmsTest
{
    //TODO: use another logger
    private static Logger _logger = Logger.getLogger("qpid.jms-client.connection");

    private final MessageFactory _messageFactory = new ProtonFactoryLoader<MessageFactory>(MessageFactory.class).loadFactory();

    @Test
    public void test() throws Exception
    {
        System.out.println("PHDEBUG " + System.getProperty("java.util.logging.config.file"));
        ConnectionImpl connection =  new ConnectionImpl("clientName", "localhost", 5672, "guest", "guest");
        connection.connect();

        SessionImpl session = connection.createSession();
        session.establish();

        SenderImpl sender = session.createSender("1","queue");
        sender.establish();

        Message message = _messageFactory.createMessage();
        AmqpValue body = new AmqpValue("Hello World!");
        message.setBody(body);
        sender.sendMessage(message);

        sender.close();

        ReceiverImpl receiver = session.createReceiver("1", "queue");
        receiver.credit(5);
        receiver.establish();
        ReceivedMessageImpl receivedMessage = receiver.receive(5000);
        receivedMessage.accept(true);

        _logger.info("=========================");
        _logger.info(receivedMessage.getMessage().getBody().toString());
        _logger.info("=========================");

        receiver.close();

        session.close();

        _logger.info("About to close " + connection);

        connection.close();
        _logger.info("Closed connection.");
    }

}
