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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
import org.apache.qpid.proton.message.Message;

public abstract class AmqpMessage
{
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

    public long getCreationTime()
    {
        return _message.getCreationTime();
    }

    public void setCreationTime(long timeInMillis)
    {
        _message.setCreationTime(timeInMillis);
    }

    public String getReplyTo()
    {
        return _message.getReplyTo();
    }

    public void setReplyTo(String replyTo)
    {
        _message.setReplyTo(replyTo);
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
