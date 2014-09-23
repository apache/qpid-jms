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

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;

public class Drain
{
    private static final String DEFAULT_USER = "guest";
    private static final String DEFAULT_PASSWORD = "guest";
    private static final int DEFAULT_PORT = 5672;
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_COUNT = 10000;

    private String _hostname;
    private int _port;
    private int _count;
    private String _username;
    private String _password;
    private String _queuePrefix;

    public Drain(int count, String hostname, int port, String queuePrefix)
    {
        _count = count;
        _hostname = hostname;
        _port = port;
        _username = DEFAULT_USER;
        _password = DEFAULT_PASSWORD;
        _queuePrefix = queuePrefix;
    }

    public void runExample()
    {
        try
        {
            //TODO: use JNDI lookup rather than direct instantiation
            JmsConnectionFactory factory = new JmsConnectionFactory("amqp://" + _hostname + ":" + _port);
            if(_queuePrefix != null)
            {
                //TODO: use URL options?
                factory.setQueuePrefix(_queuePrefix);
            }

            Connection connection = factory.createConnection(_username,_password);
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination queue = session.createQueue("myQueue");
            MessageConsumer messageConsumer = session.createConsumer(queue);

            long start = System.currentTimeMillis();

            int actualCount = 0;
            boolean deductTimeout = false;
            int timeout = 1000;
            for(int i = 1; i <= _count; i++, actualCount++)
            {
                TextMessage message = (TextMessage)messageConsumer.receive(timeout);
                if(message == null)
                {
                    System.out.println("Message not received, stopping");
                    deductTimeout = true;
                    break;
                }
                if(i % 100 == 0)
                {
                    System.out.println("Got message " + i + ":" + message.getText());
                }
            }

            long finish = System.currentTimeMillis();
            long taken = finish - start;
            if(deductTimeout)
            {
                taken -= timeout;
            }
            System.out.println("Received " + actualCount +" messages in " + taken + "ms");

            connection.close();
        }
        catch (Exception exp)
        {
            exp.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] argv) throws Exception
    {
        List<String> switches = new ArrayList<String>();
        List<String> args = new ArrayList<String>();
        for (String s : argv)
        {
            if (s.startsWith("-"))
            {
                switches.add(s);
            }
            else
            {
                args.add(s);
            }
        }

        int count = args.isEmpty() ? DEFAULT_COUNT : Integer.parseInt(args.remove(0));
        String hostname = args.isEmpty() ? DEFAULT_HOST : args.remove(0);
        int port = args.isEmpty() ? DEFAULT_PORT : Integer.parseInt(args.remove(0));
        String queuePrefix = args.isEmpty() ? null : args.remove(0);

        new Drain(count, hostname, port, queuePrefix).runExample();
    }
}