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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpSerializedObjectMessage extends AmqpObjectMessage
{
    public static final String CONTENT_TYPE = "application/x-java-serialized-object";

    public AmqpSerializedObjectMessage()
    {
        super();
        setContentType(CONTENT_TYPE);
    }

    public AmqpSerializedObjectMessage(Delivery delivery, Message message, AmqpConnection amqpConnection)
    {
        super(delivery, message, amqpConnection);
    }

    /**
     * Sets the serialized object as a data section in the underlying message, or
     * clears the body section if null.
     */
    @Override
    public void setObject(Serializable serializable) throws IOException
    {
        if(serializable == null)
        {
            //TODO: verify whether not sending a body is ok,
            //send a serialized null instead if it isn't
            getMessage().setBody(null);
        }
        else
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(serializable);
            oos.flush();
            oos.close();

            byte[] bytes = baos.toByteArray();

            getMessage().setBody(new Data(new Binary(bytes)));
        }

        //TODO: ensure content type is [still] set?
    }

    /**
     * Returns the deserialized object, or null if no object data is present.
     */
    @Override
    public Serializable getObject() throws IllegalStateException, IOException, ClassNotFoundException
    {
        Binary bin = null;

        Section body = getMessage().getBody();
        if(body == null)
        {
            return null;
        }
        else if(body instanceof Data)
        {
            bin = ((Data) body).getValue();
        }
        else
        {
            throw new IllegalStateException("Unexpected body type: " + body.getClass().getSimpleName());
        }

        if(bin == null)
        {
            return null;
        }
        else
        {
            ByteArrayInputStream bais = new ByteArrayInputStream(bin.getArray(), bin.getArrayOffset(), bin.getLength());
            ObjectInputStream ois = new ObjectInputStream(bais);

            return (Serializable) ois.readObject();
        }
    }
}
