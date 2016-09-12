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
package org.apache.qpid.jms.message.facade.test;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.message.facade.JmsMapMessageFacade;

/**
 * Simple implementation of the JmsMapMessageFacade used for testing.
 */
public class JmsTestMapMessageFacade extends JmsTestMessageFacade implements JmsMapMessageFacade {

    protected final Map<String, Object> map = new HashMap<String, Object>();

    @Override
    public JmsMsgType getMsgType() {
        return JmsMsgType.MAP;
    }

    @Override
    public JmsTestMapMessageFacade copy() {
        JmsTestMapMessageFacade copy = new JmsTestMapMessageFacade();
        copyInto(copy);
        copy.map.putAll(map);
        return copy;
    }

    @Override
    public Enumeration<String> getMapNames() {
        return Collections.enumeration(map.keySet());
    }

    @Override
    public boolean itemExists(String key) {
        return map.containsKey(key);
    }

    @Override
    public Object get(String key) {
        return map.get(key);
    }

    @Override
    public void put(String key, Object value) {
        map.put(key, value);
    }

    @Override
    public Object remove(String key) {
        return map.remove(key);
    }

    @Override
    public void clearBody() {
        map.clear();
    }

    @Override
    public boolean hasBody() {
        return !map.isEmpty();
    }
}
