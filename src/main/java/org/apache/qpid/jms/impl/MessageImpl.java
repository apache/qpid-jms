/*
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
 */
package org.apache.qpid.jms.impl;

import static org.apache.qpid.jms.impl.ClientProperties.JMS_AMQP_TTL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.engine.AmqpMessage;

public abstract class MessageImpl<T extends AmqpMessage> implements Message
{
    private static final long MAX_UINT = 0xFFFFFFFFL;
    private final T _amqpMessage;
    private final SessionImpl _sessionImpl;
    private Long _jmsExpirationFromTTL = null;
    private Destination _destination;
    private Destination _replyTo;

    /**
     * Used to record the value of JMS_AMQP_TTL property
     * if it is explicitly set by the application
     */
    private Long _propJMS_AMQP_TTL = null;

    //message to be sent
    public MessageImpl(T amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        _amqpMessage = amqpMessage;
        _sessionImpl = sessionImpl;
    }

    //message just received
    public MessageImpl(T amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination)
    {
        _amqpMessage = amqpMessage;
        _sessionImpl = sessionImpl;

        String to = _amqpMessage.getTo();
        String toTypeString = (String) _amqpMessage.getMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        _destination = sessionImpl.getDestinationHelper().decodeDestination(to, toTypeString, consumerDestination, false);

        String replyTo = _amqpMessage.getReplyTo();
        String replyToTypeString = (String) _amqpMessage.getMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        _replyTo = sessionImpl.getDestinationHelper().decodeDestination(replyTo, replyToTypeString, consumerDestination, true);

        //If we have to synthesize JMSExpiration from only the AMQP TTL header, calculate it now
        Long ttl = _amqpMessage.getTtl();
        Long absoluteExpiryTime = _amqpMessage.getAbsoluteExpiryTime();
        if(absoluteExpiryTime == null && ttl != null)
        {
            _jmsExpirationFromTTL = System.currentTimeMillis() + ttl;
        }
    }

    T getUnderlyingAmqpMessage(boolean prepareForSending)
    {
        if(prepareForSending)
        {
            return prepareUnderlyingAmqpMessageForSending(_amqpMessage);
        }
        else
        {
            return _amqpMessage;
        }
    }

    protected abstract T prepareUnderlyingAmqpMessageForSending(T amqpMessage);

    private void checkPropertyNameIsValid(String propertyName) throws IllegalArgumentException
    {
        if (propertyName == null)
        {
            throw new IllegalArgumentException("Property name must not be null");
        }
        else if (propertyName.length() == 0)
        {
            throw new IllegalArgumentException("Property name must not be the empty string");
        }

        //TODO: validate name format?
        //checkPropertyNameFormat(propertyName);
    }

    private boolean checkObjectPropertyValueIsValid(Object object) throws MessageFormatException
    {
        boolean valid = object instanceof Boolean || object instanceof Byte || object instanceof Short ||
                        object instanceof Integer || object instanceof Long || object instanceof Float ||
                        object instanceof Double || object instanceof String|| object == null;
        if(!valid)
        {
            throw new MessageFormatException("Invalid object property value type: " + object.getClass());
        }

        return true;
    }

    private void setApplicationProperty(String name, Object value) throws MessageFormatException
    {
        checkPropertyNameIsValid(name);

        if(JMS_AMQP_TTL.equals(name))
        {
            Long ttl = null;
            if(value instanceof Long)
            {
                ttl = (Long) value;
            }

            if(ttl != null && ttl >= 0 && ttl <= MAX_UINT)
            {
                _propJMS_AMQP_TTL = ttl;
            }
            else
            {
                throw new MessageFormatException(JMS_AMQP_TTL + " must be a long with value in range 0 to 2^31 - 1");
            }

            return;
        }

        checkObjectPropertyValueIsValid(value);

        _amqpMessage.setApplicationProperty(name, value);
    }

    private Object getApplicationProperty(String name)
    {
        checkPropertyNameIsValid(name);

        if(JMS_AMQP_TTL.equals(name))
        {
            return _propJMS_AMQP_TTL;
        }

        //TODO: handle non-JMS types?
        return _amqpMessage.getApplicationProperty(name);
    }


    //======= JMS Methods =======


    @Override
    public String getJMSMessageID() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long getJMSTimestamp() throws JMSException
    {
        Long timestamp = _amqpMessage.getCreationTime();
        if(timestamp == null)
        {
            return 0;
        }
        else
        {
            return timestamp.longValue();
        }
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException
    {
        if(timestamp != 0)
        {
            _amqpMessage.setCreationTime(timestamp);
        }
        else
        {
            _amqpMessage.setCreationTime(null);
        }
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public String getJMSCorrelationID() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException
    {
        return _replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException
    {
        _replyTo = replyTo;

        String replyToAddress = _sessionImpl.getDestinationHelper().decodeAddress(_replyTo);
        String typeString = _sessionImpl.getDestinationHelper().decodeTypeString(_replyTo);

        _amqpMessage.setReplyTo(replyToAddress);

        if(replyToAddress == null || typeString == null)
        {
            _amqpMessage.clearMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        }
        else
        {
            _amqpMessage.setMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, typeString);
        }
    }

    @Override
    public Destination getJMSDestination() throws JMSException
    {
        return _destination;
    }

    @Override
    public void setJMSDestination(final Destination destination) throws JMSException
    {
        _destination = destination;

        String to = _sessionImpl.getDestinationHelper().decodeAddress(destination);
        String typeString = _sessionImpl.getDestinationHelper().decodeTypeString(destination);

        _amqpMessage.setTo(to);

        if(to == null || typeString == null)
        {
            _amqpMessage.clearMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        }
        else
        {
            _amqpMessage.setMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, typeString);
        }
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException
    {
        if(getUnderlyingAmqpMessage(false).isDurable())
        {
            return DeliveryMode.PERSISTENT;
        }
        else
        {
            return DeliveryMode.NON_PERSISTENT;
        }
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException
    {
        if(DeliveryMode.PERSISTENT == deliveryMode)
        {
            getUnderlyingAmqpMessage(false).setDurable(true);
        }
        else
        {
            getUnderlyingAmqpMessage(false).setDurable(false);
        }
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public String getJMSType() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSType(String type) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long getJMSExpiration() throws JMSException
    {
        //Use absolute-expiry if present
        Long absoluteExpiry = _amqpMessage.getAbsoluteExpiryTime();
        if(absoluteExpiry != null)
        {
            return absoluteExpiry;
        }

        if(_jmsExpirationFromTTL != null)
        {
            return _jmsExpirationFromTTL;
        }

        //failing the above, there is no expiration
        return 0;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException
    {
        //clear the ttl-derived value in case it was set, we are changing to an explicit value
        _jmsExpirationFromTTL = null;

        if(expiration != 0)
        {
            _amqpMessage.setAbsoluteExpiryTime(expiration);
        }
        else
        {
            _amqpMessage.setAbsoluteExpiryTime(null);
        }
    }

    @Override
    public int getJMSPriority() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void clearProperties() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");

        //_propJMS_AMQP_TTL = null;
    }

    @Override
    public boolean propertyExists(String name) throws JMSException
    {
        if(JMS_AMQP_TTL.equals(name))
        {
            return _propJMS_AMQP_TTL != null;
        }

        return _amqpMessage.applicationPropertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Boolean)
        {
            return ((Boolean) value).booleanValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Boolean.valueOf((String) value);
        }
        else
        {
            throw new MessageFormatException("Property " + name + " of type " + value.getClass().getName()
                + " cannot be converted to boolean.");
        }
    }

    @Override
    public byte getByteProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Byte)
        {
            return ((Byte) value).byteValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Byte.valueOf((String) value).byteValue();
        }
        else
        {
            throw new MessageFormatException("Property " + name + " of type " + value.getClass().getName()
                + " cannot be converted to byte.");
        }
    }

    @Override
    public short getShortProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Short)
        {
            return ((Short) value).shortValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).shortValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Short.valueOf((String) value).shortValue();
        }
        else
        {
            throw new MessageFormatException("Property " + name + " of type " + value.getClass().getName()
                + " cannot be converted to short.");
        }
    }

    @Override
    public int getIntProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Integer)
        {
            return ((Integer) value).intValue();
        }
        else if (value instanceof Short)
        {
            return ((Short) value).intValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).intValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Integer.valueOf((String) value).intValue();
        }
        else
        {
            throw new MessageFormatException("Property " + name + " of type " + value.getClass().getName()
                + " cannot be converted to int.");
        }
    }

    @Override
    public long getLongProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Long)
        {
            return ((Long) value).longValue();
        }
        else if (value instanceof Integer)
        {
            return ((Integer) value).longValue();
        }
        else if (value instanceof Short)
        {
            return ((Short) value).longValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).longValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Long.valueOf((String) value).longValue();
        }
        else
        {
            throw new MessageFormatException("Property " + name + " of type " + value.getClass().getName()
                + " cannot be converted to long.");
        }
    }

    @Override
    public float getFloatProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Float)
        {
            return ((Float) value).floatValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Float.valueOf((String) value).floatValue();
        }
        else
        {
            throw new MessageFormatException("Property " + name + " of type " + value.getClass().getName()
                + " cannot be converted to float.");
        }
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Double)
        {
            return ((Double) value).doubleValue();
        }
        else if (value instanceof Float)
        {
            return ((Float) value).doubleValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Double.valueOf((String) value).doubleValue();
        }
        else
        {
            throw new MessageFormatException("Property " + name + " of type " + value.getClass().getName()
                + " cannot be converted to double.");
        }
    }

    @Override
    public String getStringProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if ((value instanceof String) || (value == null))
        {
            return (String) value;
        }
        else
        {
            //TODO: verify it is a JMS type?
            return value.toString();
        }
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException
    {
        //TODO: verify it is a JMS type?
        return getApplicationProperty(name);
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException
    {
        //Get the base names from the underlying AMQP message
        Set<String> underlyingApplicationPropertyNames = _amqpMessage.getApplicationPropertyNames();

        //Create a new list we can mutate
        List<String> propNames = new ArrayList<String>(underlyingApplicationPropertyNames);

        if(_propJMS_AMQP_TTL != null)
        {
            propNames.add(JMS_AMQP_TTL);
        }

        return Collections.enumeration(propNames);
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void acknowledge() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void clearBody() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }
}
