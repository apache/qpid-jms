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
package org.apache.qpid.jms.message.facade;

import java.util.Enumeration;

import javax.jms.JMSException;

/**
 * Interface for a message Facade that wraps a MapMessage style provider
 * message.
 */
public interface JmsMapMessageFacade extends JmsMessageFacade {

    /**
     * @return a deep copy of this Message Facade including a complete copy
     *         of the byte contents of the wrapped message.
     *
     * @throws JMSException if an error occurs while copying this message.
     */
    @Override
    JmsMapMessageFacade copy() throws JMSException;

    /**
     * Returns an Enumeration of all the names in the MapMessage object.
     *
     * @return an enumeration of all the names in this MapMessage
     */
    Enumeration<String> getMapNames();

    /**
     * Determines whether an item exists in this Map based message.
     *
     * @param key
     *      The entry key that is being searched for.
     *
     * @return true if the item exists in the Map, false otherwise.
     */
    boolean itemExists(String key);

    /**
     * Gets the value stored in the Map at the specified key.
     *
     * @param key
     *        the key to use to access a value in the Map.
     *
     * @return the item associated with the given key, or null if not present.
     */
    Object get(String key);

    /**
     * Sets an object value with the specified name into the Map.
     *
     * If a previous mapping for the key exists, the old value is replaced by the
     * specified value.
     *
     * If the value provided is a byte[] its entry then it is assumed that it was
     * copied by the caller and its value will not be altered by the provider.
     *
     * @param key
     *        the key to use to store the value into the Map.
     * @param value
     *        the new value to store in the element defined by the key.
     */
    void put(String key, Object value);

    /**
     * Remove the mapping for this key from the map if present.  If the value is not
     * present in the map then this method should return without error or modification
     * to the underlying map.
     *
     * @param key
     *        the key to be removed from the map if present.
     *
     * @return the object previously stored in the Map or null if none present.
     */
    Object remove(String key);

}
