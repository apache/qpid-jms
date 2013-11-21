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
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
import org.apache.qpid.proton.message.Message;

/**
 * Thread-safe (all state is guarded by the corresponding {@link AmqpConnection} monitor)
 *
 */
public abstract class AmqpMessage
{
    private final Delivery _delivery;
    private final Message _message;
    private final AmqpConnection _amqpConnection;

    private volatile MessageAnnotations _messageAnnotations;
    private volatile Map<Object,Object> _messageAnnotationsMap;

    private volatile Map<String,Object> _applicationPropertiesMap;

    /**
     * Used when creating a message that we intend to send
     */
    public AmqpMessage()
    {
        this(Proton.message(), null, null);
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

    public void setContentType(String contentType)
    {
        //TODO: do we need to synchronise this?
        _message.setContentType(contentType);
    }

    public boolean messageAnnotationExists(Object key)
    {
        //TODO: this isn't thread-safe, does it need to be?
        Map<Object,Object> msgAnnotations = _messageAnnotationsMap;
        if(msgAnnotations == null)
        {
            return false;
        }

        return msgAnnotations.containsKey(key);
    }

    public void clearMessageAnnotation(Object key)
    {
        //TODO: this isnt thread-safe, does it need to be?
        if(_messageAnnotationsMap == null)
        {
            return;
        }

        _messageAnnotationsMap.remove(key);

        //If there are now no annotations, clear the field on
        //the Proton message to avoid encoding an empty map
        if(_messageAnnotationsMap.isEmpty())
        {
            clearAllMessageAnnotations();
        }
    }

    public void clearAllMessageAnnotations()
    {
        //TODO: this isnt thread-safe, does it need to be?
        _messageAnnotations = null;
        _message.setMessageAnnotations(null);
    }

    public void setMessageAnnotation(Object key, Object value)
    {
        //TODO: this isnt thread-safe, does it need to be?
        if(_messageAnnotationsMap == null)
        {
            _messageAnnotationsMap = new HashMap<Object,Object>();
        }

        _messageAnnotationsMap.put(key, value);

        //If there were previously no annotations, we need to
        //set the related field on the Proton message now
        if(_messageAnnotations == null)
        {
            setMessageAnnotations();
        }
    }

    private void setMessageAnnotations()
    {
        //TODO: this isnt thread-safe, does it need to be?
        _messageAnnotations = new MessageAnnotations(_messageAnnotationsMap);
        _message.setMessageAnnotations(_messageAnnotations);
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

    public void setApplicationProperty(String key, Object value)
    {
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
