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

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.util.TypeConversionSupport;

public class JmsMessage implements javax.jms.Message {

    private static final String ID_PREFIX = "ID:";
    protected transient Callable<Void> acknowledgeCallback;
    protected transient JmsConnection connection;

    protected final JmsMessageFacade facade;
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
        this.readOnlyProperties = other.readOnlyBody;
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
                acknowledgeCallback.call();
            } catch (Throwable e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    @Override
    public void clearBody() throws JMSException {
        readOnlyBody = false;
        facade.clearBody();
    }

    public boolean isValidatePropertyNames() {
        return validatePropertyNames;
    }

    public void setValidatePropertyNames(boolean validatePropertyNames) {
        this.validatePropertyNames = validatePropertyNames;
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
        Object value = facade.getMessageId();
        if (value != null && !value.toString().startsWith(ID_PREFIX)) {
            value = ID_PREFIX + value;
        }

        return value == null ? null : value.toString();
    }

    @Override
    public void setJMSMessageID(String value) throws JMSException {
        facade.setMessageId(value);
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return facade.getTimestamp();
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        facade.setTimestamp(timestamp);
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return facade.getCorrelationId();
    }

    @Override
    public void setJMSCorrelationID(String correlationId) throws JMSException {
        facade.setCorrelationId(correlationId);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return facade.getCorrelationIdBytes();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationId) throws JMSException {
        facade.setCorrelationIdBytes(correlationId);
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return facade.getReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination destination) throws JMSException {
        facade.setReplyTo(JmsMessageTransformation.transformDestination(connection, destination));
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return facade.getDestination();
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        facade.setDestination(JmsMessageTransformation.transformDestination(connection, destination));
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return facade.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    @Override
    public void setJMSDeliveryMode(int mode) throws JMSException {
        facade.setPersistent(mode == DeliveryMode.PERSISTENT);
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return facade.isRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        facade.setRedelivered(redelivered);
    }

    @Override
    public String getJMSType() throws JMSException {
        return facade.getType();
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        facade.setType(type);
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return facade.getExpiration();
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        facade.setExpiration(expiration);
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return facade.getPriority();
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        facade.setPriority(priority);
    }

    @Override
    public void clearProperties() throws JMSException {
        JmsMessagePropertyIntercepter.clearProperties(facade, true);
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        try {
            checkPropertyNameIsValid(name);
        } catch (IllegalArgumentException iae) {
            return false;
        }

        return JmsMessagePropertyIntercepter.propertyExists(facade, name);
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException {
        Set<String> result = new HashSet<String>();

        Set<String> propertyNames = JmsMessagePropertyIntercepter.getPropertyNames(facade, true);
        for (String name : propertyNames) {
            try {
                checkPropertyNameIsValid(name);
            } catch (IllegalArgumentException iae) {
                // Don't add the name
                continue;
            }

            result.add(name);
        }

        return Collections.enumeration(result);
    }

    /**
     * return all property names, including standard JMS properties and JMSX
     * properties
     *
     * @return Enumeration of all property names on this message
     * @throws JMSException
     */
    public Enumeration<?> getAllPropertyNames() throws JMSException {
        Set<String> result = new HashSet<String>();
        result.addAll(JmsMessagePropertyIntercepter.getAllPropertyNames(facade));
        return Collections.enumeration(result);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        checkReadOnlyProperties();
        checkPropertyNameIsValid(name);
        checkValidObject(value);
        JmsMessagePropertyIntercepter.setProperty(facade, name, value);
    }

    protected void checkValidObject(Object value) throws MessageFormatException {
        boolean valid = value instanceof Boolean ||
                        value instanceof Byte ||
                        value instanceof Short ||
                        value instanceof Integer ||
                        value instanceof Long ||
                        value instanceof Float ||
                        value instanceof Double ||
                        value instanceof Character ||
                        value instanceof String ||
                        value == null;

        if (!valid) {
            throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
        }
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        checkPropertyNameIsValid(name);
        return JmsMessagePropertyIntercepter.getProperty(facade, name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            return false;
        }
        Boolean rc = (Boolean) TypeConversionSupport.convert(value, Boolean.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a boolean");
        }
        return rc.booleanValue();
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Byte rc = (Byte) TypeConversionSupport.convert(value, Byte.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a byte");
        }
        return rc.byteValue();
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Short rc = (Short) TypeConversionSupport.convert(value, Short.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a short");
        }
        return rc.shortValue();
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as an integer");
        }
        return rc.intValue();
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Long rc = (Long) TypeConversionSupport.convert(value, Long.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a long");
        }
        return rc.longValue();
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Float rc = (Float) TypeConversionSupport.convert(value, Float.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a float");
        }
        return rc.floatValue();
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Double rc = (Double) TypeConversionSupport.convert(value, Double.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a double");
        }
        return rc.doubleValue();
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            return null;
        }
        String rc = (String) TypeConversionSupport.convert(value, String.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a String");
        }
        return rc;
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
        setObjectProperty(name, new Float(value));
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectProperty(name, new Double(value));
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectProperty(name, value);
    }

    public Callable<Void> getAcknowledgeCallback() {
        return acknowledgeCallback;
    }

    public void setAcknowledgeCallback(Callable<Void> acknowledgeCallback) {
        this.acknowledgeCallback = acknowledgeCallback;
    }

    /**
     * Used to trigger processing required to place the message in a state where it is
     * ready to be written to the wire.  This processing can include such tasks as ensuring
     * that the proper message headers are set or compressing message bodies etc.
     *
     * @param messageId
     *        a hint that the user requested not to sent the MessageID if possible.
     * @param producerTtl
     *        the time to live value that the producer was configured with at send time.
     *
     * @throws JMSException if an error occurs while preparing the message for send.
     */
    public void onSend(Object messageId, long producerTtl) throws JMSException {
        setReadOnlyBody(true);
        setReadOnlyProperties(true);
        facade.onSend(messageId, producerTtl);
    }

    /**
     * Used to trigger processing required before dispatch of a message to its intended
     * consumer.  This method should perform any needed unmarshal or message property
     * processing prior to the message arriving at a consumer.
     *
     * @throws JMSException if an error occurs while preparing the message for dispatch.
     */
    public void onDispatch() throws JMSException {
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

    protected void checkReadOnlyProperties() throws MessageNotWriteableException {
        if (readOnlyProperties) {
            throw new MessageNotWriteableException("Message properties are read-only");
        }
    }

    protected void checkReadOnlyBody() throws MessageNotWriteableException {
        if (readOnlyBody) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }

    protected void checkWriteOnlyBody() throws MessageNotReadableException {
        if (!readOnlyBody) {
            throw new MessageNotReadableException("Message body is write-only");
        }
    }

    private void checkPropertyNameIsValid(String propertyName) throws IllegalArgumentException {
        if (propertyName == null) {
            throw new IllegalArgumentException("Property name must not be null");
        } else if (propertyName.length() == 0) {
            throw new IllegalArgumentException("Property name must not be the empty string");
        }

        if (isValidatePropertyNames()) {
            checkIdentifierLetterAndDigitRequirements(propertyName);
            checkIdentifierIsntNullTrueFalse(propertyName);
            checkIdentifierIsntLogicOperator(propertyName);
        }
    }

    private void checkIdentifierIsntLogicOperator(String identifier) {
        // Identifiers cannot be NOT, AND, OR, BETWEEN, LIKE, IN, IS, or ESCAPE.
        if ("NOT".equals(identifier) || "AND".equals(identifier) || "OR".equals(identifier) ||
            "BETWEEN".equals(identifier) || "LIKE".equals(identifier) || "IN".equals(identifier) ||
            "IS".equals(identifier) || "ESCAPE".equals(identifier)) {

            throw new IllegalArgumentException("Identifier not allowed in JMS: '" + identifier + "'");
        }
    }

    private void checkIdentifierIsntNullTrueFalse(String identifier) {
        // Identifiers cannot be the names NULL, TRUE, and FALSE.
        if ("NULL".equals(identifier) || "TRUE".equals(identifier) || "FALSE".equals(identifier)) {
            throw new IllegalArgumentException("Identifier not allowed in JMS: '" + identifier + "'");
        }
    }

    private void checkIdentifierLetterAndDigitRequirements(String identifier) {
        // An identifier is an unlimited-length sequence of letters and digits, the first of
        // which must be a letter.  A letter is any character for which the method
        // Character.isJavaLetter returns true.  This includes '_' and '$'.  A letter or digit
        // is any character for which the method Character.isJavaLetterOrDigit returns true.
        char startChar = identifier.charAt(0);
        if (!(Character.isJavaIdentifierStart(startChar))) {
            throw new IllegalArgumentException("Identifier does not begin with a valid JMS identifier start character: '" + identifier + "' ");
        }

        // JMS part character
        int length = identifier.length();
        for (int i = 1; i < length; i++) {
            char ch = identifier.charAt(i);
            if (!(Character.isJavaIdentifierPart(ch))) {
                throw new IllegalArgumentException("Identifier contains invalid JMS identifier character '" + ch + "': '" + identifier + "' ");
            }
        }
    }
}
