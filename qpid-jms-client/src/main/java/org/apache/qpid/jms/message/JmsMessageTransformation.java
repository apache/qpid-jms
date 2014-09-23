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
package org.apache.qpid.jms.message;

import java.util.Enumeration;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTemporaryQueue;
import org.apache.qpid.jms.JmsTemporaryTopic;
import org.apache.qpid.jms.JmsTopic;

/**
 * A helper class for converting normal JMS interfaces into the QpidJMS specific
 * versions.
 */
public final class JmsMessageTransformation {

    private JmsMessageTransformation() {
    }

    /**
     * Creates a an available JMS message from another provider.
     *
     * @param destination
     *        - Destination to be converted into Jms's implementation.
     * @return JmsDestination - Jms's implementation of the
     *         destination.
     * @throws JMSException
     * @throws JMSException
     *         if an error occurs
     */
    public static JmsDestination transformDestination(JmsConnection connection, Destination destination) throws JMSException {
        JmsDestination result = null;

        if (destination != null) {
            if (destination instanceof JmsDestination) {
                return (JmsDestination) destination;

            } else {
                if (destination instanceof TemporaryQueue) {
                    result = new JmsTemporaryQueue(((TemporaryQueue) destination).getQueueName());
                } else if (destination instanceof TemporaryTopic) {
                    result = new JmsTemporaryTopic(((TemporaryTopic) destination).getTopicName());
                } else if (destination instanceof Queue) {
                    result = new JmsQueue(((Queue) destination).getQueueName());
                } else if (destination instanceof Topic) {
                    result = new JmsTopic(((Topic) destination).getTopicName());
                }
            }
        }

        return result;
    }

    /**
     * Creates a fast shallow copy of the current JmsMessage or creates a
     * whole new message instance from an available JMS message from another
     * provider.
     *
     * @param message
     *        - Message to be converted into Jms's implementation.
     * @param connection
     * @return JmsMessage - Jms's implementation object of the
     *         message.
     * @throws JMSException
     *         if an error occurs
     */
    public static JmsMessage transformMessage(JmsConnection connection, Message message) throws JMSException {
        if (message instanceof JmsMessage) {
            return ((JmsMessage) message).copy();
        } else {
            JmsMessage activeMessage = null;
            JmsMessageFactory factory = connection.getMessageFactory();

            if (message instanceof BytesMessage) {
                BytesMessage bytesMsg = (BytesMessage) message;
                bytesMsg.reset();
                JmsBytesMessage msg = factory.createBytesMessage();
                try {
                    for (;;) {
                        // Reads a byte from the message stream until the stream
                        // is empty
                        msg.writeByte(bytesMsg.readByte());
                    }
                } catch (MessageEOFException e) {
                    // if an end of message stream as expected
                } catch (JMSException e) {
                }

                activeMessage = msg;
            } else if (message instanceof MapMessage) {
                MapMessage mapMsg = (MapMessage) message;
                JmsMapMessage msg = factory.createMapMessage();
                Enumeration<?> iter = mapMsg.getMapNames();

                while (iter.hasMoreElements()) {
                    String name = iter.nextElement().toString();
                    msg.setObject(name, mapMsg.getObject(name));
                }

                activeMessage = msg;
            } else if (message instanceof ObjectMessage) {
                ObjectMessage objMsg = (ObjectMessage) message;
                JmsObjectMessage msg = factory.createObjectMessage();
                msg.setObject(objMsg.getObject());
                activeMessage = msg;
            } else if (message instanceof StreamMessage) {
                StreamMessage streamMessage = (StreamMessage) message;
                streamMessage.reset();
                JmsStreamMessage msg = factory.createStreamMessage();
                Object obj = null;

                try {
                    while ((obj = streamMessage.readObject()) != null) {
                        msg.writeObject(obj);
                    }
                } catch (MessageEOFException e) {
                    // if an end of message stream as expected
                } catch (JMSException e) {
                }

                activeMessage = msg;
            } else if (message instanceof TextMessage) {
                TextMessage textMsg = (TextMessage) message;
                JmsTextMessage msg = factory.createTextMessage();
                msg.setText(textMsg.getText());
                activeMessage = msg;
            } else {
                activeMessage = factory.createTextMessage();
            }

            copyProperties(connection, message, activeMessage);

            return activeMessage;
        }
    }

    /**
     * Copies the standard JMS and user defined properties from the givem
     * message to the specified message
     *
     * @param fromMessage
     *        the message to take the properties from
     * @param toMessage
     *        the message to add the properties to
     * @throws JMSException
     */
    public static void copyProperties(JmsConnection connection, Message fromMessage, Message toMessage) throws JMSException {
        toMessage.setJMSMessageID(fromMessage.getJMSMessageID());
        toMessage.setJMSCorrelationID(fromMessage.getJMSCorrelationID());
        toMessage.setJMSReplyTo(transformDestination(connection, fromMessage.getJMSReplyTo()));
        toMessage.setJMSDestination(transformDestination(connection, fromMessage.getJMSDestination()));
        toMessage.setJMSDeliveryMode(fromMessage.getJMSDeliveryMode());
        toMessage.setJMSRedelivered(fromMessage.getJMSRedelivered());
        toMessage.setJMSType(fromMessage.getJMSType());
        toMessage.setJMSExpiration(fromMessage.getJMSExpiration());
        toMessage.setJMSPriority(fromMessage.getJMSPriority());
        toMessage.setJMSTimestamp(fromMessage.getJMSTimestamp());

        Enumeration<?> propertyNames = fromMessage.getPropertyNames();

        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement().toString();
            Object obj = fromMessage.getObjectProperty(name);
            toMessage.setObjectProperty(name, obj);
        }
    }
}
