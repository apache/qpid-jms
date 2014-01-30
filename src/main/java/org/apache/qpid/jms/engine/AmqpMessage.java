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
package org.apache.qpid.jms.engine;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
import org.apache.qpid.proton.message.Message;

public abstract class AmqpMessage
{
    static final short DEFAULT_PRIORITY = 4;

    private final Delivery _delivery;
    private final Message _message;
    private final AmqpConnection _amqpConnection;

    private volatile MessageAnnotations _messageAnnotations;
    private volatile Map<Object,Object> _messageAnnotationsMap;

    private volatile Map<String,Object> _applicationPropertiesMap;

    /**
     * Used when creating a message that we intend to send.
     * Sets the AMQP durable header to true.
     */
    public AmqpMessage()
    {
        this(Proton.message(), null, null);
        setDurable(true);
    }

    /**
     * Used when creating a message that has been received
     */
    @SuppressWarnings("unchecked")
    public AmqpMessage(Message message, Delivery delivery, AmqpConnection amqpConnection)
    {
        _delivery = delivery;
        _amqpConnection = amqpConnection;
        _message = message;

        _messageAnnotations = _message.getMessageAnnotations();
        if(_messageAnnotations != null)
        {
            _messageAnnotationsMap = _messageAnnotations.getValue();
        }

        if(_message.getApplicationProperties() != null)
        {
            _applicationPropertiesMap = _message.getApplicationProperties().getValue();
        }
    }

    Message getMessage()
    {
        return _message;
    }

    public void accept(boolean settle)
    {
        synchronized (_amqpConnection)
        {
            _delivery.disposition(Accepted.getInstance());
            if(settle)
            {
                settle();
            }
        }
    }

    public void settle()
    {
        synchronized (_amqpConnection)
        {
            _delivery.settle();
        }
    }

    /**
     * If using proton-j, returns true if locally or remotely settled.
     * If using proton-c, returns true if remotely settled.
     * TODO - remove this hack when Proton-J and -C APIs are properly aligned
     * The C API defines isSettled as being true if the delivery has been settled locally OR remotely
     */
    public boolean isSettled()
    {
        synchronized (_amqpConnection)
        {
            return _delivery.isSettled() || ((_delivery instanceof DeliveryImpl && ((DeliveryImpl)_delivery).remotelySettled()));
        }
    }

    //===== Header ======

    public void setDurable(boolean durable)
    {
        _message.setDurable(durable);
    }

    public boolean isDurable()
    {
        return _message.isDurable();
    }

    /**
     * @return the ttl in milliseconds, or null if none exists
     */
    public Long getTtl()
    {
        if(_message.getHeader() == null)
        {
            return null;
        }
        else
        {
            UnsignedInteger ttl = _message.getHeader().getTtl();
            if(ttl == null)
            {
                return null;
            }
            else
            {
                return ttl.longValue();
            }
        }
    }

    /**
     * @param timeInMillis the ttl time in milliseconds, or null to clear the field
     */
    public void setTtl(Long timeInMillis)
    {
        if(timeInMillis == null)
        {
            if(_message.getHeader() == null)
            {
                return;
            }
            else
            {
                _message.getHeader().setTtl(null);
            }
        }
        else
        {
            _message.setTtl(timeInMillis);
        }
    }

    /**
     * @return the underlying priority, or 4 (the default) if none exists
     */
    public short getPriority()
    {
        if(_message.getHeader() == null)
        {
            return DEFAULT_PRIORITY;
        }
        else
        {
            UnsignedByte priority = _message.getHeader().getPriority();
            if(priority == null)
            {
                return DEFAULT_PRIORITY;
            }
            else
            {
                return priority.shortValue();
            }
        }
    }

    /**
     * @param priority the priority, where a value of 4 clears the underlying field as it is the default
     */
    public void setPriority(short priority)
    {
        if(priority == DEFAULT_PRIORITY)
        {
            if(_message.getHeader() == null)
            {
                return;
            }
            else
            {
                _message.getHeader().setPriority(null);
            }
        }
        else
        {
            _message.setPriority(priority);
        }
    }

    //===== MessageAnnotations ======

    /**
     * @param keyName The name of the symbol key
     * @return true if an annotation exists with the provided symbol name, false otherwise
     */
    public boolean messageAnnotationExists(String keyName)
    {
        if(_messageAnnotationsMap == null)
        {
            return false;
        }

        return _messageAnnotationsMap.containsKey(Symbol.valueOf(keyName));
    }

    /**
     * @param keyName The name of the symbol key
     * @return the value of the annotation if it exists, or null otherwise
     */
    public Object getMessageAnnotation(String keyName)
    {
        if(_messageAnnotationsMap == null)
        {
            return null;
        }

        return _messageAnnotationsMap.get(Symbol.valueOf(keyName));
    }

    public void clearMessageAnnotation(String keyName)
    {
        if(_messageAnnotationsMap == null)
        {
            return;
        }

        _messageAnnotationsMap.remove(Symbol.valueOf(keyName));
    }

    /**
     * @param keyName The name of the symbol key
     * @param value the annotation value
     */
    public void setMessageAnnotation(String keyName, Object value)
    {
        if(_messageAnnotationsMap == null)
        {
            initializeUnderlyingMessageAnnotations();
        }

        _messageAnnotationsMap.put(Symbol.valueOf(keyName), value);
    }

    /**
     * Clears any previously set annotations and removes the underlying
     * message annotations section from the message
     */
    public void clearAllMessageAnnotations()
    {
        _messageAnnotationsMap = null;
        _messageAnnotations = null;
        _message.setMessageAnnotations(null);
    }

    /**
     * @return the number of MessageAnnotations.
     */
    public int getMessageAnnotationsCount()
    {
        if(_messageAnnotationsMap != null)
        {
            return _messageAnnotationsMap.size();
        }
        else
        {
            return 0;
        }
    }

    private void initializeUnderlyingMessageAnnotations()
    {
        _messageAnnotationsMap = new HashMap<Object,Object>();
        _messageAnnotations = new MessageAnnotations(_messageAnnotationsMap);
        _message.setMessageAnnotations(_messageAnnotations);
    }

    //===== Properties ======

    public String getContentType()
    {
        return _message.getContentType();
    }

    public void setContentType(String contentType)
    {
        _message.setContentType(contentType);
    }

    public String getTo()
    {
        return _message.getAddress();
    }

    public void setTo(String to)
    {
        _message.setAddress(to);
    }

    public Long getCreationTime()
    {
        if(_message.getProperties() == null)
        {
            return null;
        }
        else
        {
            Date date = _message.getProperties().getCreationTime();
            if(date == null)
            {
                return null;
            }
            else
            {
                return date.getTime();
            }
        }
    }

    public void setCreationTime(Long timeInMillis)
    {
        if(timeInMillis == null)
        {
            if(_message.getProperties() == null)
            {
                return;
            }
            else
            {
                _message.getProperties().setCreationTime(null);
            }
        }
        else
        {
            _message.setCreationTime(timeInMillis);
        }
    }

    public String getReplyTo()
    {
        return _message.getReplyTo();
    }

    public void setReplyTo(String replyTo)
    {
        _message.setReplyTo(replyTo);
    }

    /**
     * @return the expiration time in milliseconds since the Unix Epoch, or null if none exists
     */
    public Long getAbsoluteExpiryTime()
    {
        if(_message.getProperties() == null)
        {
            return null;
        }
        else
        {
            Date date = _message.getProperties().getAbsoluteExpiryTime();
            if(date == null)
            {
                return null;
            }
            else
            {
                return date.getTime();
            }
        }
    }

    /**
     * @param timeInMillis the expiration time in milliseconds since the Unix Epoch, or null to clear the field
     */
    public void setAbsoluteExpiryTime(Long timeInMillis)
    {
        if(timeInMillis == null)
        {
            if(_message.getProperties() == null)
            {
                return;
            }
            else
            {
                _message.getProperties().setAbsoluteExpiryTime(null);
            }
        }
        else
        {
            _message.setExpiryTime(timeInMillis);
        }
    }

    /**
     * Get the MessageId.
     *
     * If present, the returned object may be a String, UUID,
     * ByteBuffer (representing binary), or BigInteger (representing ulong).
     *
     * @return the messageId, or null if there isn't any
     */
    public Object getMessageId()
    {
        Object underlyingMessageId = _message.getMessageId();

        if(underlyingMessageId instanceof Binary)
        {
            return ((Binary) underlyingMessageId).asByteBuffer();
        }
        else if(underlyingMessageId instanceof UnsignedLong)
        {
            return ((UnsignedLong) underlyingMessageId).bigIntegerValue();
        }
        else
        {
            return underlyingMessageId;
        }
    }

    /**
     * Set a string message-id value on the message.
     */
    public void setMessageId(String messageId)
    {
        setUnderlyingMessageId(messageId);
    }

    /**
     * Set a uuid message-id value on the message.
     */
    public void setMessageId(UUID messageId)
    {
        setUnderlyingMessageId(messageId);
    }

    /**
     * Set an ulong (represented here as a BigInteger) message-id value on the message.
     *
     * @param messageId the value to set
     * @throws IllegalArgumentException if the value is not within the ulong range of [0 - 2^64)
     */
    public void setMessageId(BigInteger messageId) throws IllegalArgumentException
    {
        if(messageId.signum() == -1 || messageId.bitLength() > 64)
        {
            throw new IllegalArgumentException("Value \""+messageId+"\" lies outside the range [0 - 2^64).");
        }

        //TODO: add UnsignedLong.valueOf(BigInteger)?
        setUnderlyingMessageId(UnsignedLong.valueOf(messageId.toString()));
    }

    /**
     * Set a Binary (represented here as a ByteBuffer) message-id value on the message.
     *
     * @param messageId the value to set
     */
    public void setMessageId(ByteBuffer messageId)
    {
        Binary bin = Binary.create(messageId);

        setUnderlyingMessageId(bin);
    }

    private void setUnderlyingMessageId(Object messageId)
    {
        _message.setMessageId(messageId);
    }

    //===== Application Properties ======

    private void createApplicationProperties()
    {
        _applicationPropertiesMap = new HashMap<String,Object>();
        _message.setApplicationProperties(new ApplicationProperties(_applicationPropertiesMap));
    }

    public Set<String> getApplicationPropertyNames()
    {
        if(_applicationPropertiesMap != null)
        {
           return _applicationPropertiesMap.keySet();
        }
        else
        {
            return Collections.emptySet();
        }
    }

    public boolean applicationPropertyExists(String key)
    {
        if(_applicationPropertiesMap != null)
        {
           return _applicationPropertiesMap.containsKey(key);
        }
        else
        {
            return false;
        }
    }

    public Object getApplicationProperty(String key)
    {
        if(_applicationPropertiesMap != null)
        {
           return _applicationPropertiesMap.get(key);
        }
        else
        {
            return null;
        }
    }

    /**
     * @throws IllegalArgumentException if the provided key is null
     */
    public void setApplicationProperty(String key, Object value) throws IllegalArgumentException
    {
        if(key == null)
        {
            throw new IllegalArgumentException("Property key must not be null");
        }

        if(_applicationPropertiesMap == null)
        {
            createApplicationProperties();
        }

        _applicationPropertiesMap.put(key, value);
    }

    public void clearAllApplicationProperties()
    {
        _applicationPropertiesMap = null;
        _message.setApplicationProperties(null);
    }
}
