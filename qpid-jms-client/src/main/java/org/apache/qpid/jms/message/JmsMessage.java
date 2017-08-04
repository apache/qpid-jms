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

import static org.apache.qpid.jms.message.JmsMessagePropertySupport.convertPropertyTo;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.JmsAcknowledgeCallback;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;

public class JmsMessage implements javax.jms.Message {

    private static final String ID_PREFIX = "ID:";
    protected transient JmsAcknowledgeCallback acknowledgeCallback;
    protected transient JmsConnection connection;

    protected final JmsMessageFacade facade;
    protected boolean readOnly;
    protected boolean readOnlyBody;
    protected boolean readOnlyProperties;
    protected boolean validatePropertyNames = true;

    public JmsMessage(JmsMessageFacade facade) {
        this.facade = facade;
    }

    public JmsMessage copy() throws JMSException {
        JmsMessage other = new JmsMessage(facade.copy());
        other.copy(this);
        return other;
    }

    protected void copy(JmsMessage other) {
        this.readOnlyBody = other.readOnlyBody;
        this.readOnlyProperties = other.readOnlyProperties;
        this.acknowledgeCallback = other.acknowledgeCallback;
        this.connection = other.connection;
        this.validatePropertyNames = other.validatePropertyNames;
    }

    @Override
    public int hashCode() {
        Object id = facade.getMessageId();

        if (id != null) {
            return id.hashCode();
        } else {
            return super.hashCode();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        JmsMessage msg = (JmsMessage) o;
        Object oMsg = msg.facade.getMessageId();
        Object thisMsg = facade.getMessageId();

        return thisMsg != null && oMsg != null && oMsg.equals(thisMsg);
    }

    @Override
    public void acknowledge() throws JMSException {
        if (acknowledgeCallback != null) {
            try {
                acknowledgeCallback.acknowledge();
                acknowledgeCallback = null;
            } catch (Throwable e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        return true;
    }

    @Override
    public final <T> T getBody(Class<T> asType) throws JMSException {
        if (isBodyAssignableTo(asType)) {
            return doGetBody(asType);
        }

        throw new MessageFormatException("Message body cannot be read as type: " + asType);
    }

    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        return null;
    }

    @Override
    public void clearBody() throws JMSException {
        checkReadOnly();
        readOnlyBody = false;
        facade.clearBody();
    }

    public boolean isValidatePropertyNames() {
        return validatePropertyNames;
    }

    public void setValidatePropertyNames(boolean validatePropertyNames) {
        this.validatePropertyNames = validatePropertyNames;
    }

    public boolean isReadOnly() {
        return this.readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnlyBody() {
        return this.readOnlyBody;
    }

    public void setReadOnlyBody(boolean readOnlyBody) {
        this.readOnlyBody = readOnlyBody;
    }

    public boolean isReadOnlyProperties() {
        return this.readOnlyProperties;
    }

    public void setReadOnlyProperties(boolean readOnlyProperties) {
        this.readOnlyProperties = readOnlyProperties;
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        String id = facade.getMessageId();
        if (id != null && !id.startsWith(ID_PREFIX)) {
            id = ID_PREFIX + id;
        }

        return id;
    }

    @Override
    public void setJMSMessageID(String value) throws JMSException {
        checkReadOnly();
        facade.setMessageId(value);
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return facade.getTimestamp();
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        checkReadOnly();
        facade.setTimestamp(timestamp);
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return facade.getCorrelationId();
    }

    @Override
    public void setJMSCorrelationID(String correlationId) throws JMSException {
        checkReadOnly();
        facade.setCorrelationId(correlationId);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return facade.getCorrelationIdBytes();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationId) throws JMSException {
        checkReadOnly();
        facade.setCorrelationIdBytes(correlationId);
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return facade.getReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination destination) throws JMSException {
        checkReadOnly();
        facade.setReplyTo(JmsMessageTransformation.transformDestination(connection, destination));
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return facade.getDestination();
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        checkReadOnly();
        facade.setDestination(JmsMessageTransformation.transformDestination(connection, destination));
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return facade.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    @Override
    public void setJMSDeliveryMode(int mode) throws JMSException {
        checkReadOnly();
        switch (mode) {
            case DeliveryMode.PERSISTENT:
                facade.setPersistent(true);
                break;
            case DeliveryMode.NON_PERSISTENT:
                facade.setPersistent(false);
                break;
            default:
                throw new JMSException(String.format("Invalid DeliveryMode specific: %d", mode));
        }
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return facade.isRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        checkReadOnly();
        facade.setRedelivered(redelivered);
    }

    @Override
    public String getJMSType() throws JMSException {
        return facade.getType();
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        checkReadOnly();
        facade.setType(type);
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return facade.getExpiration();
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        checkReadOnly();
        facade.setExpiration(expiration);
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return facade.getPriority();
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        checkReadOnly();

        if (priority < 0 || priority > 9) {
            throw new JMSException(String.format("Priority value given {%d} is out of range (0..9)", priority));
        }

        facade.setPriority(priority);
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return facade.getDeliveryTime();
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        checkReadOnly();
        facade.setDeliveryTime(deliveryTime, true);
    }

    @Override
    public void clearProperties() throws JMSException {
        checkReadOnly();
        JmsMessagePropertyIntercepter.clearProperties(this, true);
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return JmsMessagePropertyIntercepter.propertyExists(this, name);
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException {
        return Collections.enumeration(JmsMessagePropertyIntercepter.getPropertyNames(this, true));
    }

    /**
     * return all property names, including standard JMS properties and JMSX
     * properties
     *
     * @return Enumeration of all property names on this message
     *
     * @throws JMSException if an error occurs while reading the properties from the Message.
     */
    public Enumeration<?> getAllPropertyNames() throws JMSException {
        Set<String> result = new HashSet<String>();
        result.addAll(JmsMessagePropertyIntercepter.getAllPropertyNames(this));
        return Collections.enumeration(result);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        checkReadOnly();
        JmsMessagePropertyIntercepter.setProperty(this, name, value);
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return JmsMessagePropertyIntercepter.getProperty(this, name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return convertPropertyTo(name, getObjectProperty(name), Boolean.class);
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return convertPropertyTo(name, getObjectProperty(name), Byte.class);
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return convertPropertyTo(name, getObjectProperty(name), Short.class);
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return convertPropertyTo(name, getObjectProperty(name), Integer.class);
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return convertPropertyTo(name, getObjectProperty(name), Long.class);
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return convertPropertyTo(name, getObjectProperty(name), Float.class);
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return convertPropertyTo(name, getObjectProperty(name), Double.class);
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return convertPropertyTo(name, getObjectProperty(name), String.class);
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setObjectProperty(name, Boolean.valueOf(value));
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setObjectProperty(name, Byte.valueOf(value));
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setObjectProperty(name, Short.valueOf(value));
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setObjectProperty(name, Integer.valueOf(value));
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setObjectProperty(name, Long.valueOf(value));
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setObjectProperty(name, Float.valueOf(value));
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectProperty(name, Double.valueOf(value));
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectProperty(name, value);
    }

    public JmsAcknowledgeCallback getAcknowledgeCallback() {
        return acknowledgeCallback;
    }

    public void setAcknowledgeCallback(JmsAcknowledgeCallback jmsAcknowledgeCallback) {
        this.acknowledgeCallback = jmsAcknowledgeCallback;
    }

    /**
     * Used to trigger processing required to place the message in a state where it is
     * ready to be written to the wire.  This processing can include such tasks as ensuring
     * that the proper message headers are set or compressing message bodies etc.  During this
     * call the message is placed in a read-only mode and will not be returned to a writable
     * state until send completion is triggered.
     *
     * @param producerTtl
     *        the time to live value that the producer was configured with at send time.
     *
     * @throws JMSException if an error occurs while preparing the message for send.
     */
    public void onSend(long producerTtl) throws JMSException {
        setReadOnly(true);
        facade.onSend(producerTtl);
    }

    /**
     * Used to trigger processing required to place the message into a writable state once
     * again following completion of the send operation.
     */
    public void onSendComplete() {
        setReadOnly(false);
    }

    /**
     * Used to trigger processing required before dispatch of a message to its intended
     * consumer.  This method should perform any needed decoding or message property
     * processing prior to the message arriving at a consumer.
     *
     * @throws JMSException if an error occurs while preparing the message for dispatch.
     */
    public void onDispatch() throws JMSException {
        setReadOnly(false);
        setReadOnlyBody(true);
        setReadOnlyProperties(true);
        facade.onDispatch();
    }

    public JmsConnection getConnection() {
        return connection;
    }

    public void setConnection(JmsConnection connection) {
        this.connection = connection;
    }

    public JmsMessageFacade getFacade() {
        return this.facade;
    }

    public boolean isExpired() {
        long expireTime = facade.getExpiration();
        return expireTime > 0 && System.currentTimeMillis() > expireTime;
    }

    @Override
    public String toString() {
        return "JmsMessage { " + facade + " }";
    }

    //----- State validation methods -----------------------------------------//

    protected void checkReadOnly() throws MessageNotWriteableException {
        if (readOnly) {
            throw new MessageNotWriteableException("Message is currently read-only");
        }
    }

    protected void checkReadOnlyProperties() throws MessageNotWriteableException {
        if (readOnly || readOnlyProperties) {
            throw new MessageNotWriteableException("Message properties are read-only");
        }
    }

    protected void checkReadOnlyBody() throws MessageNotWriteableException {
        if (readOnly || readOnlyBody) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }

    protected void checkWriteOnlyBody() throws MessageNotReadableException {
        if (!readOnlyBody) {
            throw new MessageNotReadableException("Message body is write-only");
        }
    }
}
