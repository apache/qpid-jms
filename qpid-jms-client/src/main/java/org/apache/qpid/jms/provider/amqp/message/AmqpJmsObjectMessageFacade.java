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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;

import java.io.IOException;
import java.io.Serializable;

import javax.jms.JMSException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.facade.JmsObjectMessageFacade;
import org.apache.qpid.jms.policy.JmsDeserializationPolicy;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS ObjectMessage
 * type.
 */
public class AmqpJmsObjectMessageFacade extends AmqpJmsMessageFacade implements JmsObjectMessageFacade {

    private AmqpObjectTypeDelegate delegate;
    private JmsDeserializationPolicy deserializationPolicy;

    @Override
    public void initialize(AmqpConnection connection) {
        super.initialize(connection);
        initDelegate(connection.isObjectMessageUsesAmqpTypes());
    }

    @Override
    public void initialize(AmqpConsumer consumer) {
        super.initialize(consumer);
        deserializationPolicy = consumer.getResourceInfo().getDeserializationPolicy();
        boolean javaSerialized = AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.equals(getContentType());
        initDelegate(!javaSerialized);
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
        AmqpJmsObjectMessageFacade copy = new AmqpJmsObjectMessageFacade();
        copy.deserializationPolicy = deserializationPolicy;
        copy.initDelegate(isAmqpTypedEncoding());
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
    public boolean hasBody() {
        return delegate.hasBody();
    }

    @Override
    public void onSend(long producerTtl) throws JMSException {
        super.onSend(producerTtl);
        delegate.onSend();
    }

    @Override
    public JmsObjectMessage asJmsMessage() {
        return new JmsObjectMessage(this);
    }

    void setUseAmqpTypedEncoding(boolean useAmqpTypedEncoding) throws JMSException {
        if (useAmqpTypedEncoding != delegate.isAmqpTypeEncoded()) {
            try {
                Serializable existingObject = delegate.getObject();

                AmqpObjectTypeDelegate newDelegate = null;
                if (useAmqpTypedEncoding) {
                    newDelegate = new AmqpTypedObjectDelegate(this);
                } else {
                    newDelegate = new AmqpSerializedObjectDelegate(this, deserializationPolicy);
                }

                newDelegate.setObject(existingObject);

                delegate = newDelegate;
            } catch (ClassNotFoundException | IOException e) {
                throw JmsExceptionSupport.create(e);
            }
        }
    }

    private void initDelegate(boolean useAmqpTypes) {
        if (!useAmqpTypes) {
            delegate = new AmqpSerializedObjectDelegate(this, deserializationPolicy);
        } else {
            delegate = new AmqpTypedObjectDelegate(this);
        }
    }

    AmqpObjectTypeDelegate getDelegate() {
        return delegate;
    }
}
