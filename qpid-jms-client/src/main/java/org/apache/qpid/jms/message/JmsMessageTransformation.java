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
package org.apache.qpid.jms.message;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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

    private static JmsUnresolvedDestinationTransformer unresolvedDestinationHandler =
        new JmsDefaultUnresolvedDestinationTransformer();

    /**
     * Creates a an available JMS message from another provider.
     *
     * @param connection
     *      The Connection instance that is requesting the transformation.
     * @param destination
     *      Destination to be converted into Jms's implementation.
     *
     * @return JmsDestination - Jms's implementation of the destination.
     *
     * @throws JMSException if an error occurs during the transformation.
     */
    public static JmsDestination transformDestination(JmsConnection connection, Destination destination) throws JMSException {
        JmsDestination result = null;

        if (destination != null) {

            if (destination instanceof JmsDestination) {
                result = (JmsDestination) destination;
            } else if (destination instanceof Queue && destination instanceof Topic) {
                String queueName = ((Queue) destination).getQueueName();
                String topicName = ((Topic) destination).getTopicName();
                if (queueName != null && topicName == null) {
                    result = new JmsQueue(queueName);
                } else if (queueName == null && topicName != null) {
                    result = new JmsTopic(topicName);
                } else {
                    result = unresolvedDestinationHandler.transform(destination);
                }
            } else {
                if (destination instanceof TemporaryQueue) {
                    result = new JmsTemporaryQueue(((TemporaryQueue) destination).getQueueName());
                } else if (destination instanceof TemporaryTopic) {
                    result = new JmsTemporaryTopic(((TemporaryTopic) destination).getTopicName());
                } else if (destination instanceof Queue) {
                    result = new JmsQueue(((Queue) destination).getQueueName());
                } else if (destination instanceof Topic) {
                    result = new JmsTopic(((Topic) destination).getTopicName());
                } else {
                    result = unresolvedDestinationHandler.transform(destination);
                }
            }
        }

        return result;
    }

    /**
     * Creates a new JmsMessage object and populates it using the details of
     * the given Message.
     *
     * @param connection
     *        The JmsConnection where this transformation is being initiated.
     * @param message
     *        Message to be converted into the clients implementation.
     *
     * @return JmsMessage
     *         The client's implementation object for the incoming message.
     *
     * @throws JMSException if an error occurs during the transform.
     */
    public static JmsMessage transformMessage(JmsConnection connection, Message message) throws JMSException {
        JmsMessage jmsMessage = null;
        JmsMessageFactory factory = connection.getMessageFactory();

        if (message instanceof BytesMessage) {
            BytesMessage bytesMsg = (BytesMessage) message;
            bytesMsg.reset();
            JmsBytesMessage msg = factory.createBytesMessage();
            try {
                for (;;) {
                    // Reads a byte from the message stream until the stream is empty
                    msg.writeByte(bytesMsg.readByte());
                }
            } catch (MessageEOFException e) {
                // Indicates all the bytes have been read from the source.
            }

            jmsMessage = msg;
        } else if (message instanceof MapMessage) {
            MapMessage mapMsg = (MapMessage) message;
            JmsMapMessage msg = factory.createMapMessage();
            Enumeration<?> iter = mapMsg.getMapNames();

            while (iter.hasMoreElements()) {
                String name = iter.nextElement().toString();
                msg.setObject(name, mapMsg.getObject(name));
            }

            jmsMessage = msg;
        } else if (message instanceof ObjectMessage) {
            ObjectMessage objMsg = (ObjectMessage) message;
            JmsObjectMessage msg = factory.createObjectMessage();
            msg.setObject(objMsg.getObject());
            jmsMessage = msg;
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
                // Indicates all the stream values have been read from the source.
            }

            jmsMessage = msg;
        } else if (message instanceof TextMessage) {
            TextMessage textMsg = (TextMessage) message;
            JmsTextMessage msg = factory.createTextMessage();
            msg.setText(textMsg.getText());
            jmsMessage = msg;
        } else {
            jmsMessage = factory.createMessage();
        }

        copyProperties(connection, message, jmsMessage);

        return jmsMessage;
    }

    /**
     * Copies the standard JMS and user defined properties from the given source
     * message to the specified target message.  The copy can only handle the JMS
     * specific message properties and known JMS Headers, any headers that are
     * specific to the foreign message may be lost if not returned directly via
     * the <code>propertyNames</code> method.
     *
     * @param connection
     *        The Connection instance that is requesting the transformation.
     * @param source
     *        the message to take the properties from
     * @param target
     *        the message to add the properties to
     *
     * @throws JMSException if an error occurs during the copy of message properties.
     */
    public static void copyProperties(JmsConnection connection, Message source, JmsMessage target) throws JMSException {
        target.setJMSMessageID(source.getJMSMessageID());
        target.setJMSCorrelationID(source.getJMSCorrelationID());
        target.setJMSReplyTo(transformDestination(connection, source.getJMSReplyTo()));
        target.setJMSDestination(transformDestination(connection, source.getJMSDestination()));
        target.setJMSDeliveryMode(source.getJMSDeliveryMode());
        target.setJMSDeliveryTime(getForeignMessageDeliveryTime(source));
        target.setJMSRedelivered(source.getJMSRedelivered());
        target.setJMSType(source.getJMSType());
        target.setJMSExpiration(source.getJMSExpiration());
        target.setJMSPriority(source.getJMSPriority());
        target.setJMSTimestamp(source.getJMSTimestamp());

        Enumeration<?> propertyNames = source.getPropertyNames();

        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement().toString();
            Object obj = source.getObjectProperty(name);
            target.setObjectProperty(name, obj);
        }
    }

    public static void setUnresolvedDestinationHandler(JmsUnresolvedDestinationTransformer handler) {
        unresolvedDestinationHandler = handler;
    }

    public static JmsUnresolvedDestinationTransformer getUnresolvedDestinationTransformer() {
        if (unresolvedDestinationHandler == null) {
            unresolvedDestinationHandler = new JmsDefaultUnresolvedDestinationTransformer();
        }

        return unresolvedDestinationHandler;
    }

    private static long getForeignMessageDeliveryTime(Message foreignMessage) throws JMSException {
        // Verify if the getJMSDeliveryTime method exists, i.e the foreign provider isn't only JMS 1.1.
        Method deliveryTimeMethod = null;
        try {
            Class<?> clazz = foreignMessage.getClass();
            Method method = clazz.getMethod("getJMSDeliveryTime", (Class[]) null);
            if (!Modifier.isAbstract(method.getModifiers())) {
                deliveryTimeMethod = method;
            }
        } catch (NoSuchMethodException e) {
            // Assume its a JMS 1.1 Message, we will return 0.
        }

        if (deliveryTimeMethod != null) {
            // Method exists, isn't abstract, so use it.
            return foreignMessage.getJMSDeliveryTime();
        }

        return 0;
    }
}
