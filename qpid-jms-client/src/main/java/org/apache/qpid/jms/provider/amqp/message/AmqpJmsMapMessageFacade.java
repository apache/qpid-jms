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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;

import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.jms.message.facade.JmsMapMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS MapMessage
 * type.
 */
public class AmqpJmsMapMessageFacade extends AmqpJmsMessageFacade implements JmsMapMessageFacade {

    private Map<String,Object> messageBodyMap;

    /**
     * Create a new facade ready for sending.
     *
     * @param connection
     *        the AmqpConnection that under which this facade was created.
     */
    public AmqpJmsMapMessageFacade(AmqpConnection connection) {
        super(connection);
        initializeEmptyBody();
        setMessageAnnotation(JMS_MSG_TYPE, JMS_MAP_MESSAGE);
    }

    /**
     * Creates a new Facade around an incoming AMQP Message for dispatch to the
     * JMS Consumer instance.
     *
     * @param consumer
     *        the consumer that received this message.
     * @param message
     *        the incoming Message instance that is being wrapped.
     */
    @SuppressWarnings("unchecked")
    public AmqpJmsMapMessageFacade(AmqpConsumer consumer, Message message) {
        super(consumer, message);

        Section body = getAmqpMessage().getBody();
        if (body == null) {
            initializeEmptyBody();
        } else if (body instanceof AmqpValue) {
            Object o = ((AmqpValue) body).getValue();
            if (o == null) {
                initializeEmptyBody();
            } else if (o instanceof Map) {
                messageBodyMap = (Map<String, Object>) o;
            } else {
                throw new IllegalStateException("Unexpected message body type: " + body.getClass().getSimpleName());
            }
        } else {
            throw new IllegalStateException("Unexpected message body type: " + body.getClass().getSimpleName());
        }
    }

    /**
     * @return the appropriate byte value that indicates the type of message this is.
     */
    @Override
    public byte getJmsMsgType() {
        return JMS_MAP_MESSAGE;
    }

    @Override
    public AmqpJmsMapMessageFacade copy() {
        AmqpJmsMapMessageFacade copy = new AmqpJmsMapMessageFacade(connection);
        copyInto(copy);
        copy.messageBodyMap.putAll(messageBodyMap);
        return copy;
    }

    @Override
    public Enumeration<String> getMapNames() {
        return Collections.enumeration(messageBodyMap.keySet());
    }

    @Override
    public boolean itemExists(String key) {
        return messageBodyMap.containsKey(key);
    }

    @Override
    public Object get(String key) {
        Object value = messageBodyMap.get(key);
        if (value instanceof Binary) {
            // Copy to a byte[], ensure we copy only the required portion.
            Binary bin = ((Binary) value);
            value = Arrays.copyOfRange(bin.getArray(), bin.getArrayOffset(), bin.getLength());
        }

        return value;
    }

    @Override
    public void put(String key, Object value) {
        Object entry = value;
        if (value instanceof byte[]) {
            entry = new Binary((byte[]) value);
        }

        messageBodyMap.put(key, entry);
    }

    @Override
    public Object remove(String key) {
        return messageBodyMap.remove(key);
    }

    @Override
    public void clearBody() {
        messageBodyMap.clear();
    }

    private void initializeEmptyBody() {
        // Using LinkedHashMap because AMQP map equality considers order,
        // so we should behave in as predictable a manner as possible
        messageBodyMap = new LinkedHashMap<String, Object>();
        getAmqpMessage().setBody(new AmqpValue(messageBodyMap));
    }
}
