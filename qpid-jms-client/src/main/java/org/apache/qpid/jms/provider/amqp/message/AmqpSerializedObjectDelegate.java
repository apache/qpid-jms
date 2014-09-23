/**
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.qpid.jms.util.ClassLoadingAwareObjectInputStream;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS ObjectMessage
 * type.
 */
public class AmqpSerializedObjectDelegate implements AmqpObjectTypeDelegate {

    public static final String CONTENT_TYPE = "application/x-java-serialized-object";

    private final Message message;

    /**
     * Create a new delegate that uses Java serialization to store the message content.
     *
     * @param message
     *        the AMQP message instance where the object is to be stored / read.
     */
    public AmqpSerializedObjectDelegate(Message message) {
        this.message = message;
        this.message.setContentType(CONTENT_TYPE);
    }

    @Override
    public Serializable getObject() throws IOException, ClassNotFoundException {
        Binary bin = null;

        Section body = message.getBody();
        if (body == null) {
            return null;
        } else if (body instanceof Data) {
            bin = ((Data) body).getValue();
        } else {
            throw new IllegalStateException("Unexpected body type: " + body.getClass().getSimpleName());
        }

        if (bin == null) {
            return null;
        } else {
            Serializable serialized = null;

            try (ByteArrayInputStream bais = new ByteArrayInputStream(bin.getArray(), bin.getArrayOffset(), bin.getLength());
                 ClassLoadingAwareObjectInputStream objIn = new ClassLoadingAwareObjectInputStream(bais)) {

                serialized = (Serializable) objIn.readObject();
            }

            return serialized;
        }
    }

    @Override
    public void setObject(Serializable value) throws IOException {
        if(value == null) {
            // TODO: verify whether not sending a body is ok,
            //       send a serialized null instead if it isn't
            message.setBody(null);
        } else {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {

               oos.writeObject(value);
               oos.flush();
               oos.close();

               byte[] bytes = baos.toByteArray();
               message.setBody(new Data(new Binary(bytes)));
            }
        }

        // TODO: ensure content type is [still] set?
    }
}
