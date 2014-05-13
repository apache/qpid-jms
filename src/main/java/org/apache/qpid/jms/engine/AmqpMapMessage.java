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
package org.apache.qpid.jms.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpMapMessage extends AmqpMessage
{
    private volatile Map<String,Object> _messageBodyMap;

    public AmqpMapMessage()
    {
        super();
        initialiseMessageBodyMap();
    }

    @SuppressWarnings("unchecked")
    public AmqpMapMessage(Message message, Delivery delivery, AmqpConnection amqpConnection)
    {
        super(message, delivery, amqpConnection);

        Section body = getMessage().getBody();
        if(body == null)
        {
            initialiseMessageBodyMap();
        }
        else if(body instanceof AmqpValue)
        {
            Object o = ((AmqpValue) body).getValue();
            if(o == null)
            {
                initialiseMessageBodyMap();
            }
            else
            {
                _messageBodyMap = (Map<String, Object>) o;
            }
        }
        else
        {
            throw new IllegalStateException("Unexpected message body type: " + body.getClass().getSimpleName());
        }
    }

    private void initialiseMessageBodyMap()
    {
        _messageBodyMap= new HashMap<String,Object>();
        getMessage().setBody(new AmqpValue(_messageBodyMap));
    }

    /**
     * Returns a Set view of the keys contained in this map. The set is backed by the map, so changes to the map are reflected in the set, and vice-versa.
     *
     * @return a set of the keys in the underlying map
     */
    public Set<String> getMapKeys()
    {
        return _messageBodyMap.keySet();
    }

    /**
     * Associates the specified value with the specified key in this map message.
     *
     * If a previous mapping for the key exists, the old value is replaced by the specified value and the old value is returned.
     * @param key the key for the mapping
     * @param value the value for the mapping
     * @return the old value if one exists for this key, or null if there was none.
     */
    public Object setMapEntry(String key, Object value)
    {
        return _messageBodyMap.put(key, value);
    }

    /**
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     *
     * @param key the key for the mapping
     * @return the value if one exists for this key, or null if there was none.
     */
    public Object getMapEntry(String key)
    {
        return _messageBodyMap.get(key);
    }

    /**
     * Clears all existing map entries.
     */
    public void clearMapEntries()
    {
        _messageBodyMap.clear();
    }

    /**
     * Check if a given key exists in the map.
     *
     * @param key the key to check
     * @return
     */
    public boolean mapEntryExists(String key)
    {
        return _messageBodyMap.containsKey(key);
    }
}
