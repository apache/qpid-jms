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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.qpid.jms.policy.JmsDeserializationPolicy;
import org.apache.qpid.jms.util.ClassLoadingAwareObjectInputStream;
import org.apache.qpid.jms.util.ClassLoadingAwareObjectInputStream.TrustedClassFilter;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS ObjectMessage
 * type.
 */
public class AmqpSerializedObjectDelegate implements AmqpObjectTypeDelegate, TrustedClassFilter {

    static final Data NULL_OBJECT_BODY;
    static
    {
        byte[] bytes;
        try {
            bytes = getSerializedBytes(null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialise null object body", e);
        }

        NULL_OBJECT_BODY = new Data(new Binary(bytes));
    }

    private final AmqpJmsMessageFacade parent;
    private final JmsDeserializationPolicy deserializationPolicy;
    private boolean localContent;

    /**
     * Create a new delegate that uses Java serialization to store the message content.
     *
     * @param parent
     *        the AMQP message facade instance where the object is to be stored / read.
     * @param deserializationPolicy
     *        the JmsDeserializationPolicy that is used to validate the security of message
     *        content, may be null (e.g on new outgoing messages).
     */
    public AmqpSerializedObjectDelegate(AmqpJmsMessageFacade parent, JmsDeserializationPolicy deserializationPolicy) {
        this.parent = parent;
        this.parent.setContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
        this.deserializationPolicy = deserializationPolicy;
    }

    private static byte[] getSerializedBytes(Serializable value) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            oos.writeObject(value);
            oos.flush();
            oos.close();

            return baos.toByteArray();
        }
    }

    @Override
    public Serializable getObject() throws IOException, ClassNotFoundException {
        Binary binary = null;

        Section body = parent.getBody();

        if (body == null || body == NULL_OBJECT_BODY) {
            return null;
        } else if (body instanceof Data) {
            binary = ((Data) body).getValue();
        } else {
            throw new IllegalStateException("Unexpected body type: " + body.getClass().getSimpleName());
        }

        if (binary == null) {
            return null;
        } else {
            Serializable serialized = null;

            try (ByteArrayInputStream bais = new ByteArrayInputStream(binary.getArray(), binary.getArrayOffset(), binary.getLength());
                 ClassLoadingAwareObjectInputStream objIn = new ClassLoadingAwareObjectInputStream(bais, this)) {

                serialized = (Serializable) objIn.readObject();
            }

            return serialized;
        }
    }

    @Override
    public void setObject(Serializable value) throws IOException {
        if (value == null) {
            parent.setBody(NULL_OBJECT_BODY);
        } else {
            byte[] bytes = getSerializedBytes(value);
            parent.setBody(new Data(new Binary(bytes)));
        }

        localContent = true;
    }

    @Override
    public void onSend() {
        parent.setContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
        if (parent.getBody() == null) {
            parent.setBody(NULL_OBJECT_BODY);
        }
    }

    @Override
    public void copyInto(AmqpObjectTypeDelegate copy) throws Exception {
        if (!(copy instanceof AmqpSerializedObjectDelegate)) {
            copy.setObject(getObject());
        } else {
            AmqpSerializedObjectDelegate target = (AmqpSerializedObjectDelegate) copy;

            target.localContent = localContent;

            // Copy the already encoded message body if it exists, subsequent gets
            // will deserialize the data so no mutations can occur.
            target.parent.setBody(parent.getBody());
        }
    }

    @Override
    public boolean isAmqpTypeEncoded() {
        return false;
    }

    @Override
    public boolean isTrusted(Class<?> clazz) {
        if (!localContent && deserializationPolicy != null) {
            return deserializationPolicy.isTrustedType(parent.getConsumerDestination(), clazz);
        } else {
            return true;
        }
    }

    @Override
    public boolean hasBody() {
        try {
            return getObject() != null;
        } catch (Exception e) {
            return false;
        }
    }
}
