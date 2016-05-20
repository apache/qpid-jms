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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.decodeMessage;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.encodeMessage;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

import io.netty.buffer.ByteBuf;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS ObjectMessage
 * type.
 */
public class AmqpTypedObjectDelegate implements AmqpObjectTypeDelegate {

    static final AmqpValue NULL_OBJECT_BODY = new AmqpValue(null);

    private final Message message;
    private final AtomicReference<Section> cachedReceivedBody = new AtomicReference<Section>();
    private ByteBuf messageBytes;

    /**
     * Create a new delegate that uses Java serialization to store the message content.
     *
     * @param message
     *        the AMQP message instance where the object is to be stored / read.
     * @param messageBytes
     *        the raw bytes that comprise the AMQP message that was received.
     */
    public AmqpTypedObjectDelegate(Message message, ByteBuf messageBytes) {
        this.message = message;
        this.message.setContentType(null);
        this.messageBytes = messageBytes;

        // We will decode the body on each access, so clear the current value
        // so we don't carry along unneeded bloat.
        if (messageBytes != null) {
            cachedReceivedBody.set(message.getBody());
        }
    }

    @Override
    public Serializable getObject() throws IOException, ClassNotFoundException {
        Section body = cachedReceivedBody.getAndSet(null);

        if (body == null) {
            if (messageBytes != null) {
                body = decodeMessage(messageBytes).getBody();
            } else {
                body = message.getBody();
            }
        }

        if (body == null) {
            return null;
        } else if (body instanceof AmqpValue) {
            // TODO: This is assuming the object can be immediately returned, and is
            //       deeply Serializable. We will actually have to ensure elements are
            //       Serializable and e.g convert the Uint/Ubyte etc wrappers.
            return (Serializable) ((AmqpValue) body).getValue();
        } else if (body instanceof Data) {
            // TODO: return as byte[]? ByteBuffer?
            throw new UnsupportedOperationException("Data support still to be added");
        } else if (body instanceof AmqpSequence) {
            // TODO: This is assuming the object can be immediately returned, and is
            //       deeply Serializable. We will actually have to ensure elements are
            //       Serializable and e.g convert the Uint/Ubyte etc wrappers.
            return (Serializable) ((AmqpSequence) body).getValue();
        } else {
            throw new IllegalStateException("Unexpected body type: " + body.getClass().getSimpleName());
        }
    }

    @Override
    public void setObject(Serializable value) throws IOException {
        cachedReceivedBody.set(null);

        if (value == null) {
            message.setBody(NULL_OBJECT_BODY);
            messageBytes = null;
        } else if (isSupportedAmqpValueObjectType(value)) {
            Message transfer = Message.Factory.create();

            // Exchange the incoming body value for one that is created from encoding
            // and decoding the value.
            transfer.setBody(new AmqpValue(value));
            messageBytes = encodeMessage(transfer);
            transfer = decodeMessage(messageBytes);
            messageBytes = null;

            // This step requires a heavy-weight operation of both encoding and decoding the
            // incoming body value in order to create a copy such that changes to the original
            // do not affect the stored value.  In the future it makes sense to try to enhance
            // proton such that we can encode the body and use those bytes directly on the
            // message as it is being sent.

            message.setBody(transfer.getBody());
        } else {
            // TODO: Data and AmqpSequence?
            throw new IllegalArgumentException("Encoding this object type with the AMQP type system is not supported: " + value.getClass().getName());
        }
    }

    @Override
    public void onSend() {
        message.setContentType(null);
        if (message.getBody() == null) {
            message.setBody(NULL_OBJECT_BODY);
        }
    }

    private boolean isSupportedAmqpValueObjectType(Serializable serializable) {
        // TODO: augment supported types to encode as an AmqpValue?
        return serializable instanceof String ||
               serializable instanceof Map<?,?> ||
               serializable instanceof List<?> ||
               serializable.getClass().isArray();
    }

    @Override
    public boolean isAmqpTypeEncoded() {
        return true;
    }
}
