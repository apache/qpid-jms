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
package org.apache.qpid.jms.impl.temp;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.impl.ConnectionImpl;

public class Spout
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
    private boolean _persistent;

    public Spout(int count, String hostname, int port, boolean persistent)
    {
        _count = count;
        _hostname = hostname;
        _port = port;
        _persistent = persistent;
        _username = DEFAULT_USER;
        _password = DEFAULT_PASSWORD;
    }

    public void runTest()
    {
        try
        {
            Connection connection = new ConnectionImpl("spout", _hostname, _port, _username, _password);
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination queue = session.createQueue("myQueue");
            MessageProducer messageProducer = session.createProducer(queue);

            int dekiveryMode = _persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;

            long start = System.currentTimeMillis();
            for(int i = 1; i <= _count; i++)
            {
                TextMessage message = session.createTextMessage("Hello world!");
                messageProducer.send(message, dekiveryMode, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

                if(i % 100 == 0)
                {
                    System.out.println("Sent message " + i + ":" + message.getText());
                }
            }

            long finish = System.currentTimeMillis();
            long taken = finish - start;
            System.out.println("Sent " + _count +" messages in " + taken + "ms");

            connection.close();
            System.exit(0);//TODO: remove
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
        boolean persistent = switches.contains("-p");

        new Spout(count, hostname, port, persistent).runTest();
    }
}