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
import java.util.Iterator;
import java.util.Map;
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
import org.apache.qpid.jms.meta.JmsMessageId;
import org.apache.qpid.jms.util.TypeConversionSupport;

public class JmsMessage implements javax.jms.Message {

    protected transient Callable<Void> acknowledgeCallback;
    protected transient JmsConnection connection;

    protected final JmsMessageFacade facade;
    protected boolean readOnlyBody;
    protected boolean readOnlyProperties;

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
    }

    @Override
    public int hashCode() {
        String id = null;
        try {
            id = getJMSMessageID();
        } catch (JMSException e) {
        }

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
        JmsMessageId oMsg = null;
        JmsMessageId thisMsg = null;

        thisMsg = facade.getMessageId();
        oMsg = msg.facade.getMessageId();

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

    public boolean isReadOnlyBody() {
        return this.readOnlyBody;
    }

    public void setReadOnlyBody(boolean readOnlyBody) {
        this.readOnlyBody = readOnlyBody;
    }

    public void setReadOnlyProperties(boolean readOnlyProperties) {
        this.readOnlyProperties = readOnlyProperties;
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        if (facade.getMessageId() == null) {
            return null;
        }
        return facade.getMessageId().toString();
    }

    @Override
    public void setJMSMessageID(String value) throws JMSException {
        if (value != null) {
            JmsMessageId id = new JmsMessageId(value);
            facade.setMessageId(id);
        } else {
            facade.setMessageId(null);
        }
    }

    public void setJMSMessageID(JmsMessageId messageId) throws JMSException {
        facade.setMessageId(messageId);
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
        return this.isRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        this.setRedelivered(redelivered);
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
        byte scaled = 0;

        if (priority < 0) {
            scaled = 0;
        } else if (priority > 9) {
            scaled = 9;
        } else {
            scaled = (byte) priority;
        }

        facade.setPriority(scaled);
    }

    @Override
    public void clearProperties() throws JMSException {
        facade.clearProperties();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return JmsMessagePropertyIntercepter.propertyExists(facade, name);
    }

    /**
     * Returns an unmodifiable Map containing the properties contained within the message.
     *
     * @return unmodifiable Map of the current properties in the message.
     *
     * @throws JMSException if there is an error accessing the message properties.
     */
    public Map<String, Object> getProperties() throws JMSException {
        return Collections.unmodifiableMap(facade.getProperties());
    }

    /**
     * Allows for a direct put of an Object value into the message properties.
     *
     * This method bypasses the normal JMS type checking for properties being set on
     * the message and should be used with great care.
     *
     * @param key
     *        the property name to use when setting the value.
     * @param value
     *        the value to insert into the message properties.
     *
     * @throws JMSException if an error occurs while accessing the Message properties.
     */
    public void setProperty(String key, Object value) throws JMSException {
        this.facade.setProperty(key, value);
    }

    /**
     * Returns the Object value referenced by the given key.
     *
     * @param key
     *        the name of the property being accessed.
     *
     * @return the value stored at the given location or null if non set.
     *
     * @throws JMSException if an error occurs while accessing the Message properties.
     */
    public Object getProperty(String key) throws JMSException {
        return this.facade.getProperty(key);
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException {
        Set<String> result = new HashSet<String>(facade.getProperties().keySet());
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
        Set<String> result = new HashSet<String>(facade.getProperties().keySet());
        result.addAll(JmsMessagePropertyIntercepter.getAllPropertyNames());
        return Collections.enumeration(result);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        checkReadOnlyProperties();
        checkPropertyNameIsValid(name);
        checkValidObject(value);
        JmsMessagePropertyIntercepter.setProperty(facade, name, value);
    }

    public void setProperties(Map<String, Object> properties) throws JMSException {
        for (Iterator<Map.Entry<String, Object>> iter = properties.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<String, Object> entry = iter.next();
            setObjectProperty(entry.getKey(), entry.getValue());
        }
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
        if (name == null) {
            throw new NullPointerException("Property name cannot be null");
        }

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
     * Send operation event listener. Used to get the message ready to be sent.
     *
     * @throws JMSException
     */
    public void onSend() throws JMSException {
        setReadOnlyBody(true);
        setReadOnlyProperties(true);
        facade.onSend();
    }

    public JmsConnection getConnection() {
        return connection;
    }

    public void setConnection(JmsConnection connection) {
        this.connection = connection;
    }

    public boolean isExpired() throws JMSException {
        long expireTime = facade.getExpiration();
        return expireTime > 0 && System.currentTimeMillis() > expireTime;
    }

    public void incrementRedeliveryCount() {
         facade.setRedeliveryCounter(facade.getRedeliveryCounter() + 1);
    }

    public JmsMessageFacade getFacade() {
        return this.facade;
    }

    public boolean isRedelivered() throws JMSException {
        return facade.isRedelivered();
    }

    public void setRedelivered(boolean redelivered) throws JMSException {
        if (redelivered) {
            if (!isRedelivered()) {
                facade.setRedeliveryCounter(1);
            }
        } else {
            if (isRedelivered()) {
                facade.setRedeliveryCounter(0);
            }
        }
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

        checkIdentifierFormat(propertyName);
    }

    private void checkIdentifierFormat(String identifier) throws IllegalArgumentException {
        checkIdentifierLetterAndDigitRequirements(identifier);
        checkIdentifierIsntNullTrueFalse(identifier);
        checkIdentifierIsntLogicOperator(identifier);
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
