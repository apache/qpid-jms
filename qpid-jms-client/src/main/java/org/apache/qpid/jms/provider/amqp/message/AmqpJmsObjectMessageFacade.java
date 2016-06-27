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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;

import java.io.IOException;
import java.io.Serializable;

import javax.jms.JMSException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.facade.JmsObjectMessageFacade;
import org.apache.qpid.jms.policy.JmsDeserializationPolicy;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.message.Message;

import io.netty.buffer.ByteBuf;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS ObjectMessage
 * type.
 */
public class AmqpJmsObjectMessageFacade extends AmqpJmsMessageFacade implements JmsObjectMessageFacade {

    private AmqpObjectTypeDelegate delegate;

    private final JmsDeserializationPolicy deserializationPolicy;

    /**
     * Creates a new facade instance for outgoing message
     *
     * @param connection
     *        the AmqpConnection that under which this facade was created.
     * @param isAmqpTypeEncoded
     *        controls the type used to encode the body.
     */
    public AmqpJmsObjectMessageFacade(AmqpConnection connection, boolean isAmqpTypeEncoded) {
        this(connection, isAmqpTypeEncoded, null);
    }

    private AmqpJmsObjectMessageFacade(AmqpConnection connection, boolean isAmqpTypeEncoded, JmsDeserializationPolicy deserializationPolicy) {
        super(connection);
        this.deserializationPolicy = deserializationPolicy;

        setMessageAnnotation(JMS_MSG_TYPE, JMS_OBJECT_MESSAGE);
        initDelegate(isAmqpTypeEncoded, null);
    }

    /**
     * Creates a new Facade around an incoming AMQP Message for dispatch to the
     * JMS Consumer instance.
     *
     * @param consumer
     *        the consumer that received this message.
     * @param message
     *        the incoming Message instance that is being wrapped.
     * @param messageBytes
     *        a copy of the raw bytes of the incoming message.
     */
    public AmqpJmsObjectMessageFacade(AmqpConsumer consumer, Message message, ByteBuf messageBytes) {
        super(consumer, message);
        deserializationPolicy = consumer.getResourceInfo().getDeserializationPolicy();

        boolean javaSerialized = AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.equals(message.getContentType());
        initDelegate(!javaSerialized, messageBytes);
    }

    /**
     * @return the appropriate byte value that indicates the type of message this is.
     */
    @Override
    public byte getJmsMsgType() {
        return JMS_OBJECT_MESSAGE;
    }

    public boolean isAmqpTypedEncoding() {
        return delegate.isAmqpTypeEncoded();
    }

    @Override
    public AmqpJmsObjectMessageFacade copy() throws JMSException {
        AmqpJmsObjectMessageFacade copy = new AmqpJmsObjectMessageFacade(connection, isAmqpTypedEncoding(), deserializationPolicy);
        copyInto(copy);
        try {
            delegate.copyInto(copy.delegate);
        } catch (Exception ex) {
            throw JmsExceptionSupport.create(ex);
        }
        return copy;
    }

    @Override
    public Serializable getObject() throws IOException, ClassNotFoundException {
        return delegate.getObject();
    }

    @Override
    public void setObject(Serializable value) throws IOException {
        delegate.setObject(value);
    }

    @Override
    public void clearBody() {
        try {
            setObject(null);
        } catch (IOException e) {
        }
    }

    @Override
    public void onSend(long producerTtl) throws JMSException {
        super.onSend(producerTtl);
        delegate.onSend();
    }

    void setUseAmqpTypedEncoding(boolean useAmqpTypedEncoding) throws JMSException {
        if (useAmqpTypedEncoding != delegate.isAmqpTypeEncoded()) {
            try {
                Serializable existingObject = delegate.getObject();

                AmqpObjectTypeDelegate newDelegate = null;
                if (useAmqpTypedEncoding) {
                    newDelegate = new AmqpTypedObjectDelegate(this, null);
                } else {
                    newDelegate = new AmqpSerializedObjectDelegate(this, null, deserializationPolicy);
                }

                newDelegate.setObject(existingObject);

                delegate = newDelegate;
            } catch (ClassNotFoundException | IOException e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    private void initDelegate(boolean useAmqpTypes, ByteBuf messageBytes) {
        if (!useAmqpTypes) {
            delegate = new AmqpSerializedObjectDelegate(this, messageBytes, deserializationPolicy);
        } else {
            delegate = new AmqpTypedObjectDelegate(this, messageBytes);
        }
    }

    AmqpObjectTypeDelegate getDelegate() {
        return delegate;
    }
}
