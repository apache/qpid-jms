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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;

public class AmqpObjectMessageAmqpTypedDelegate implements AmqpObjectMessageDelegate
{
    private AmqpObjectMessage _parent;

    public AmqpObjectMessageAmqpTypedDelegate(AmqpObjectMessage amqpObjectMessage)
    {
        _parent = amqpObjectMessage;
    }

    /**
     * Sets the Serializable object as an AmqpValue/Data/AmqpSequence section
     * in the underlying message, or clears the body section if null.
     */
    @Override
    public void setObject(Serializable serializable) throws IOException
    {
        if(serializable == null)
        {
            //TODO: verify whether not sending a body is OK, send some form of
            //null (AmqpValue containing null) instead if it isn't?
            _parent.getMessage().setBody(null);
        }
        else if(isSupportedAmqpValueObjectType(serializable))
        {
            //TODO: This is a temporary hack, we actually need to take a snapshot of the object at this point in time, not simply set the object itself into the Proton message.
            //We will need to encode it now, first to save the snapshot to send, and also to verify up front that we can actually send it later.

            //Even if we do that we would currently then need to decode it later to set the body to send, unless we augment Proton to allow setting the bytes directly.
            //We will always need to decode bytes to return a snapshot from getObject(). We will need to save the bytes somehow to support that on received messages.
            _parent.getMessage().setBody(new AmqpValue(serializable));
        }
        else //TODO: Data and AmqpSequence?
        {
            throw new IllegalArgumentException("Encoding this object type with the AMQP type system is not supported: " + serializable.getClass().getName());
        }

        //TODO: ensure content type is not set (assuming we aren't using data sections)?
    }

    private boolean isSupportedAmqpValueObjectType(Serializable serializable)
    {
        //TODO: augment supported types to encode as an AmqpValue?
        return serializable instanceof Map<?,?> || serializable instanceof List<?> || serializable.getClass().isArray();
    }

    /**
     * Returns the deserialized object, or null if no object data is present.
     */
    @Override
    public Serializable getObject() throws IllegalStateException, ClassCastException
    {
        //TODO: this should actually return a snapshot of the object, so we
        //need to save the bytes so we can return an equal/unmodified object later

        Section body = _parent.getMessage().getBody();
        if(body == null)
        {
            return null;
        }
        else if(body instanceof AmqpValue)
        {
            //TODO: This is assuming the object can be immediately returned, and is Serializable.
            //We will actually have to ensure elements are Serializable and e.g convert the Uint/Ubyte etc wrappers.
            return (Serializable) ((AmqpValue) body).getValue();
        }
        else if(body instanceof Data)
        {
            //TODO: return as byte[]? ByteBuffer?
            throw new UnsupportedOperationException("Data support still to be added");
        }
        else if(body instanceof AmqpSequence)
        {
            //TODO: return as list?
            throw new UnsupportedOperationException("AmqpSequence support still to be added");
        }
        else
        {
            throw new IllegalStateException("Unexpected body type: " + body.getClass().getSimpleName());
        }
    }
}
