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
package org.apache.qpid.jms.message.foreign;

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.facade.test.JmsTestMessageFactory;

/**
 * Test class used to simulate receipt of a JMS Message from another
 * client library.
 */
public class ForeignJmsMessage implements Message {

    protected final JmsMessage message;

    public ForeignJmsMessage() {
        this.message = new JmsTestMessageFactory().createMessage();
    }

    protected ForeignJmsMessage(JmsMessage message) {
        this.message = message;
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        return message.getJMSMessageID();
    }

    @Override
    public void setJMSMessageID(String messageId) throws JMSException {
        message.setJMSMessageID(messageId);
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return message.getJMSTimestamp();
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        message.setJMSTimestamp(timestamp);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return message.getJMSCorrelationIDAsBytes();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        message.setJMSCorrelationIDAsBytes(correlationID);
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        message.setJMSCorrelationID(correlationID);
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return message.getJMSCorrelationID();
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return message.getJMSReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        message.setJMSReplyTo(replyTo);
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return message.getJMSDestination();
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        message.setJMSDestination(destination);
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return message.getJMSDeliveryMode();
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        message.setJMSDeliveryMode(deliveryMode);
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return message.getJMSRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        message.setJMSRedelivered(redelivered);
    }

    @Override
    public String getJMSType() throws JMSException {
        return message.getJMSType();
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        message.setJMSType(type);
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return message.getJMSExpiration();
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        message.setJMSExpiration(expiration);
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return message.getJMSPriority();
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        message.setJMSPriority(priority);
    }

    @Override
    public void clearProperties() throws JMSException {
        message.clearProperties();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return message.propertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return message.getBooleanProperty(name);
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return getByteProperty(name);
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return getShortProperty(name);
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return getIntProperty(name);
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return message.getLongProperty(name);
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return message.getFloatProperty(name);
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return message.getDoubleProperty(name);
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return message.getStringProperty(name);
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return message.getObjectProperty(name);
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException {
        return message.getPropertyNames();
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        message.setBooleanProperty(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        message.setByteProperty(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        message.setShortProperty(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        message.setIntProperty(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        message.setLongProperty(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        message.setFloatProperty(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        message.setDoubleProperty(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        message.setStringProperty(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        message.setObjectProperty(name, value);
    }

    @Override
    public void acknowledge() throws JMSException {
        message.acknowledge();
    }

    @Override
    public void clearBody() throws JMSException {
        message.clearBody();
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return message.getJMSDeliveryTime();
    }

    @Override
    public void setJMSDeliveryTime(long delay) throws JMSException {
        message.setJMSDeliveryTime(delay);
    }

    @Override
    public <T> T getBody(Class<T> asType) throws JMSException {
        return message.getBody(asType);
    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        return message.isBodyAssignableTo(target);
    }
}
