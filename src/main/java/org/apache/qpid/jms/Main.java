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

/**
 * VERY simply POC for a JMS-like client based on Proton-J
 *
 * Requires an AMQP 1.0 'broker' a localhost:5672 with a
 * node called "queue" to which a message can be sent
 * and received
 *
 */
public class Main
{
    //TODO: use another logger
    private static Logger _logger = Logger.getLogger("qpid.jms-client.connection");

    public static void main(String[] args) throws Exception
    {
        ConnectionImpl connection =  new ConnectionImpl("clientName", "localhost", 5672, "guest", "guest");
        connection.connect();

        SessionImpl session = connection.createSession();
        session.establish();

        SenderImpl sender = session.createSender("1","queue");
        sender.establish();

        MessageFactory messageFactory =  new ProtonFactoryLoader<MessageFactory>(MessageFactory.class).loadFactory();
        Message message = messageFactory.createMessage();
        AmqpValue body = new AmqpValue("Hello World!");
        message.setBody(body);
        sender.sendMessage(message);

        sender.close();

        ReceiverImpl receiver = session.createReceiver("1", "queue");
        receiver.credit(5);
        receiver.establish();
        ReceivedMessageImpl receivedMessage = receiver.receive(5000);
        receivedMessage.accept(true);

        System.out.println("=========================");
        System.out.println(receivedMessage.getMessage().getBody());
        System.out.println("=========================");
        receiver.close();

        session.close();

        connection.close();

        //HACK: just exit as we have not bothered to ensure use of
        //Daemon threads or ensure that all threads have exited.
        System.exit(0);

        //      Old Single-Thread Playing
        //      =======================
        //
        //        MessageFactory messageFactory = conn.getMessageFactory();
        //        Message message = messageFactory.createMessage();
        //        AmqpValue body = new AmqpValue("Hello World!");
        //        message.setBody(body);
        //        
        //        _logger.log(Level.FINEST, "Connected");
        //        AmqpSession s = conn.createSession();
        //        s.establish();
        //        _logger.log(Level.FINEST, "Session created");
        //        AmqpSender snd = s.createAmqpSender("1","queue");
        //        snd.establish();
        //        _logger.log(Level.FINEST, "Sender created");
        //        AmqpSentMessage sentMessage = snd.sendMessage(message);
        //        //snd.flush();
        //        sentMessage.waitUntilAccepted();
        //        sentMessage.settle();
        //        
        //        _logger.log(Level.FINEST, "Message sent");
        //        snd.close();
        //        _logger.log(Level.FINEST, "Sender closed");        
        //
        //        AmqpReceiver rcv = s.createAmqpReceiver("1", "queue");
        //        _logger.log(Level.FINEST, "Receiver created"); 
        //        rcv.credit(5);
        //        AmqpReceivedMessage receivedMessage = rcv.receive(3000l);
        //        System.out.println(receivedMessage.getMessage().getBody());
        //        _logger.log(Level.FINEST, "Message received"); 
        //        receivedMessage.accept();
        //        receivedMessage.waitUntilSettled();
        //        
        //        s.close();
        //        _logger.log(Level.FINEST, "Session closed");
        //        conn.close();
        //        _logger.log(Level.FINEST, "Connection closed");


        //      Old Messenger Playing
        //      =======================
        //
        //        Messenger m = new MessengerImpl();
        //        m.start();
        //        
        //        MessageImpl message = new MessageImpl();
        //        message.setBody(new AmqpValue("Hello World!"));
        //        message.setAddress("amqp://" + IP_ADDR + ":5672/queue");
        //        m.put(message);
        //        m.send();
    }

}
