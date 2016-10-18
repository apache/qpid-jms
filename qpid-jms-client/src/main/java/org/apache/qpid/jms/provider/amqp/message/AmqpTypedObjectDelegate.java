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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;

import io.netty.buffer.ByteBuf;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS ObjectMessage
 * type.
 */
public class AmqpTypedObjectDelegate implements AmqpObjectTypeDelegate {

    static final AmqpValue NULL_OBJECT_BODY = new AmqpValue(null);

    private ByteBuf encodedBody;
    private final AmqpJmsMessageFacade parent;

    /**
     * Create a new delegate that uses Java serialization to store the message content.
     *
     * @param parent
     *        the AMQP message facade instance where the object is to be stored / read.
     */
    public AmqpTypedObjectDelegate(AmqpJmsMessageFacade parent) {
        this.parent = parent;
        this.parent.setContentType(null);

        // Create a duplicate of the message body for decode on read attempts
        if (parent.getBody() != null) {
            encodedBody = AmqpCodec.encode(parent.getBody());
        }
    }

    @Override
    public Serializable getObject() throws IOException, ClassNotFoundException {
        Section body = null;

        if (encodedBody != null) {
            body = AmqpCodec.decode(encodedBody);
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
        if (value == null) {
            parent.setBody(NULL_OBJECT_BODY);
            encodedBody = null;
        } else if (isSupportedAmqpValueObjectType(value)) {
            // Exchange the incoming body value for one that is created from encoding
            // and decoding the value. Save the bytes for subsequent getObject and
            // copyInto calls to use.
            encodedBody = AmqpCodec.encode(new AmqpValue(value));
            Section decodedBody = AmqpCodec.decode(encodedBody);

            // This step requires a heavy-weight operation of both encoding and decoding the
            // incoming body value in order to create a copy such that changes to the original
            // do not affect the stored value, and also verifies we can actually encode it at all
            // now instead of later during send. In the future it makes sense to try to enhance
            // proton such that we can encode the body and use those bytes directly on the
            // message as it is being sent.

            parent.setBody(decodedBody);
        } else {
            // TODO: Data and AmqpSequence?
            throw new IllegalArgumentException("Encoding this object type with the AMQP type system is not supported: " + value.getClass().getName());
        }
    }

    @Override
    public void onSend() {
        parent.setContentType(null);
        if (parent.getBody() == null) {
            parent.setBody(NULL_OBJECT_BODY);
        }
    }

    @Override
    public void copyInto(AmqpObjectTypeDelegate copy) throws Exception {
        if (!(copy instanceof AmqpTypedObjectDelegate)) {
            copy.setObject(getObject());
        } else {
            AmqpTypedObjectDelegate target = (AmqpTypedObjectDelegate) copy;

            // If there ever was a body then we will have a snapshot of it and we can
            // be sure that our state is correct.
            if (encodedBody != null) {
                // If we have any body bytes just duplicate those and let the next get
                // decode them into the returned object payload value.
                target.encodedBody = encodedBody.duplicate();

                // Internal message body copy to satisfy sends. This is safe since the body was set
                // from a copy (decoded from the bytes) to ensure it is a snapshot. Also safe for
                // gets as they will use the message bytes (or cached body if set) to return the object.
                target.parent.setBody(parent.getBody());
            }
        }
    }

    @Override
    public boolean isAmqpTypeEncoded() {
        return true;
    }

    @Override
    public boolean hasBody() {
        try {
            return getObject() != null;
        } catch (Exception e) {
            return false;
        }
    }

    //----- Internal implementation ------------------------------------------//

    private boolean isSupportedAmqpValueObjectType(Serializable serializable) {
        // TODO: augment supported types to encode as an AmqpValue?
        return serializable instanceof String ||
               serializable instanceof Map<?,?> ||
               serializable instanceof List<?> ||
               serializable.getClass().isArray();
    }
}
