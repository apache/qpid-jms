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
package org.apache.qpid.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

public class Client {
    private static final int DELIVERY_MODE = DeliveryMode.NON_PERSISTENT;

    public static void main(String[] args) throws Exception {
        try {
            // The configuration for the Qpid InitialContextFactory has been supplied in
            // a jndi.properties file in the classpath, which results in it being picked
            // up automatically by the InitialContext constructor.
            Context context = new InitialContext();

            ConnectionFactory factory = (ConnectionFactory) context.lookup("myFactoryLookup");
            Destination queue = (Destination) context.lookup("myQueueLookup");

            Connection connection = factory.createConnection(System.getProperty("USER"), System.getProperty("PASSWORD"));
            connection.setExceptionListener(new MyExceptionListener());
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            //Creates a message and temporary queue to send to and from.
            int random = (int) (Math.random()*3);
            TextMessage messageToBeSent;
            if (random == 0) {
                messageToBeSent = session.createTextMessage("first example message");
            } else if (random == 1) {
                messageToBeSent = session.createTextMessage("second example message");
            } else {
                messageToBeSent = session.createTextMessage("third example message");
            }

            TemporaryQueue tempQueue = session.createTemporaryQueue();
            messageToBeSent.setJMSReplyTo(tempQueue);
            MessageProducer messageProducer = session.createProducer(queue);

            //Send the message
            messageProducer.send(messageToBeSent, DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            System.out.println("[CLIENT] The message with text \"" + messageToBeSent.getText() +"\" has been sent.");

            MessageConsumer messageConsumer = session.createConsumer(tempQueue);

            //Receive the server response
            TextMessage receivedMessage = (TextMessage) messageConsumer.receive(1000);
            if (receivedMessage != null) {
                System.out.println("[CLIENT] Response from server received.");
            } else {
                System.out.println("[CLIENT] Response not received within timeout, stopping.");
            }

            //Display response and close client.
            System.out.println("[CLIENT] Here is the interpreted message:\n" + receivedMessage.getText() + "\n[CLIENT] Quitting Client.");
            connection.close();
            System.exit(1);

        } catch (Exception exp) {
            System.out.println("[CLIENT] Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static class MyExceptionListener implements ExceptionListener {
        @Override
        public void onException(JMSException exception) {
            System.out.println("[CLIENT] Connection ExceptionListener fired, exiting.");
            exception.printStackTrace(System.out);
            System.exit(1);
        }
    }
}