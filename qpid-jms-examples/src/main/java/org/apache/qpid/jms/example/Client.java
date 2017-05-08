
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner;

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
            
            //Create message and temporary queue to send to and from.
            Message messageToBeSent = userInterface(session);
            TemporaryQueue tempQ = session.createTemporaryQueue();
            messageToBeSent.setJMSReplyTo(tempQ);

            MessageProducer messageProducer = session.createProducer(queue);
            
            //Send the message
            messageProducer.send(messageToBeSent, DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            System.out.println("[CLIENT] The message has been sent.");
            
            
            MessageConsumer messageConsumer = session.createConsumer(tempQ);
            long start = System.currentTimeMillis();
            boolean deductTimeout = false;
            int timeout = 1000;
            
            //Receive the server response
            TextMessage newMessage = (TextMessage) messageConsumer.receive(timeout);
            if (newMessage != null) {
            	System.out.print("[CLIENT] Response from server received in ");
            } else {
                System.out.println("[CLIENT] Response not received within timeout, stopping.");
                deductTimeout = true;
            }
                
            long finish = System.currentTimeMillis();
            long taken = finish - start;
            if (deductTimeout) {
                taken -= timeout;
            }
            if (newMessage != null) {
            	System.out.println(taken + "ms");
            }
            
            //Display response and close client.
            System.out.println("[CLIENT] Here is the interpreted message:\n" + newMessage.getText() + "\n[CLIENT] Quitting Client.");
            connection.close();
            System.exit(1);
            
        } catch (Exception exp) {
            System.out.println("[CLIENT] Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        }
    }
    
    //UI to generate message
    private static Message userInterface(Session session) throws Exception {
    	System.out.print("From this client class, you can specify what you want the server to do.\n"
    					+ "Choose an option:\n"
    					+ "(1) Capitalize an inputted text message\n"
    					+ "(2) Sort an inputted array of integers\n"
    					+ "(3) Sort a randomly generated array of integers\n"
    					+ "Your input: ");
    	Scanner key = new Scanner(System.in);
    	String userResponse = key.nextLine();
    	
    	//Errorcheck user response
    	while (!userResponse.equals("1") && !userResponse.equals("2") && !userResponse.equals("3")) {
    		System.out.print("Invalid option. Repeat input: ");
    		userResponse = key.nextLine();
    	}
    	
    	if (userResponse.equals("1")) {
    		return capitalize(key, session);
    	} else if (userResponse.equals("2")) {
    		return sort(key, session, false);
    	} else {
    		return sort(key, session, true);
    	}
    }
    
    //Generates a text message to capitalize
    private static Message capitalize(Scanner key, Session session) throws Exception {
    	System.out.print("Input the text message you want to be capitalized: ");
    	String userResponse = key.nextLine();
    	
    	//Errorcheck user response
    	while (userResponse.trim().equals("")) {
    		System.out.print("Input can not be blank. Try again: ");
    		userResponse = key.nextLine();
    	}
    	key.close();
    	Message message = session.createTextMessage(userResponse);
    	message.setStringProperty("FUNCTION", "capitalize");
    	return message;
    }
    
    //Generates a list of integers to sort
    private static Message sort(Scanner key, Session session, boolean isRandom) throws Exception {
    	ArrayList<Integer> AL = new ArrayList<Integer>();
    	String userResponse;
    	
    	//If the list will be created by the user
    	if (!isRandom) {
	    	int inputCount = 1;
	    	System.out.println("Input each array element, or type \"done\" to stop.");
	    	while (true) {
	    		System.out.print("Input " + inputCount + ": ");
	    		userResponse = key.nextLine();
	    		
	    		//Errorcheck user response
	    		if (isNumeric(userResponse)) {
	    			AL.add(Integer.parseInt(userResponse));
	    			inputCount++;
	    		}
	    		else if (userResponse.equalsIgnoreCase("done")) {
	    			if (inputCount == 1) {
	    				System.out.println("There must be at least one number to send.");
	    			}
	    			else {
	    				break;
	    			}
	    		}
	    		else {
	    			System.out.println("The input must either be a number input, or the terminating word \"done\".");
	    		}
	    	}
	    	
	    //If the list will be randomly generated
    	} else {
    		System.out.print("How many random integers will be generated?\nYour input: ");
    		userResponse = key.nextLine();
    		
    		//Errorcheck user response
    		while (true) {
    			if (isNumeric(userResponse)) {
    				if (Integer.parseInt(userResponse) > 0) {
    					break;
    				}
    			}
    			System.out.print("Input must be positive integer. Try again: ");
    			userResponse = key.nextLine();
    		}
    		for (int i = 0; i < Integer.parseInt(userResponse); i++) {
        		AL.add((int) (100*Math.random()));
    		}
    	}
    	key.close();
    	Message message = session.createObjectMessage((Serializable) AL);
    	message.setStringProperty("FUNCTION", "sort");
    	return message;
    }
    
    private static boolean isNumeric(String str) {  
		try {
			Integer.parseInt(str);  
		}
		catch(NumberFormatException nfe) {
			return false;  
		}
		return true;
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
