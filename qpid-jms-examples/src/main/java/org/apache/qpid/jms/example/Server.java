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
import javax.jms.TextMessage;
import javax.jms.ObjectMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.ArrayList;
import java.util.Collections;

public class Server {

    public static void main(String[] args) throws Exception {
	    	System.out.println("[SERVER] Awaiting Message...");
	
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
	            
	
	            MessageConsumer messageConsumer = session.createConsumer(queue);
	            while (true) {
	            
		            long start = System.currentTimeMillis();
		            
		            //Receive a message
		            Message receivedMessage = messageConsumer.receive(0);
		                
		            long finish = System.currentTimeMillis();
		            long taken = finish - start;
	
		            System.out.println("[SERVER] Received the message in " + taken + "ms");
		            
		            //Create new message to return to client
		            Destination replyDestination = receivedMessage.getJMSReplyTo();
		            TextMessage newMessage = session.createTextMessage(interpretMessage(receivedMessage));
		            MessageProducer messageProducer = session.createProducer(null);
		            newMessage.setJMSDestination(replyDestination);
		            messageProducer.send(replyDestination, newMessage, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
		            System.out.println("[SERVER] The message has been interpreted and sent back to the client.");
	            }
	            
	            
	            
	        } catch (Exception exp) {
	            System.out.println("[SERVER] Caught exception, exiting.");
	            exp.printStackTrace(System.out);
	            System.exit(1);
	        }
    }
    
    //Interpret received message and create a new one
    private static String interpretMessage(Message receivedMessage) throws Exception
    {
    	//If the message will be capitalized
    	if (receivedMessage.getStringProperty("FUNCTION").equals("capitalize")) {
    		return ((TextMessage) receivedMessage).getText().toUpperCase();
    		
    	//If the message will be sorted
    	} else if (receivedMessage.getStringProperty("FUNCTION").equals("sort")) {
    		ObjectMessage receivedObject = (ObjectMessage) receivedMessage;
    		if (receivedObject.getObject() instanceof ArrayList) {
    			ArrayList<Integer> newList = (ArrayList<Integer>) receivedObject.getObject();
    			Collections.sort(newList);
    			return newList.toString();
    		}
    	}
    	return null;
    }

    private static class MyExceptionListener implements ExceptionListener {
        @Override
        public void onException(JMSException exception) {
            System.out.println("[SERVER] Connection ExceptionListener fired, exiting.");
            exception.printStackTrace(System.out);
            System.exit(1);
        }
    }
}