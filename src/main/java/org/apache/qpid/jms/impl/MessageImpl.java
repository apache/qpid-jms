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

import java.util.Collections;
import java.util.Enumeration;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.engine.AmqpMessage;

public abstract class MessageImpl<T extends AmqpMessage> implements Message
{
    private final T _amqpMessage;
    private final SessionImpl _sessionImpl;
    private Destination _destination;

    public MessageImpl(T amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        _amqpMessage = amqpMessage;
        _sessionImpl = sessionImpl;

        String to = _amqpMessage.getTo();
        if(to != null)
        {
            String typeString = (String) _amqpMessage.getMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
            _destination = sessionImpl.getDestinationHelper().decodeDestination(to, typeString);
        }
        else
        {
            //TODO:
            //Message doesn't have a To. If this message was received via a
            //consumer (i.e we aren't creating this message to send), as a fallback
            //we could set the Destination used to create the consumer itself. That
            //responsibility might fall to the consumer though.
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
        checkObjectPropertyValueIsValid(value);

        _amqpMessage.setApplicationProperty(name, value);
    }

    private Object getApplicationProperty(String name)
    {
        checkPropertyNameIsValid(name);

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
        return _amqpMessage.getCreationTime();
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException
    {
        _amqpMessage.setCreationTime(timestamp);
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
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
    }

    @Override
    public boolean propertyExists(String name) throws JMSException
    {
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
        return Collections.enumeration(_amqpMessage.getApplicationPropertyNames());
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
