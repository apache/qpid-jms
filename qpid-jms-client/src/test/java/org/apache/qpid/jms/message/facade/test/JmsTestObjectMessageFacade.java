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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.qpid.jms.message.facade.JmsObjectMessageFacade;
import org.apache.qpid.jms.util.ClassLoadingAwareObjectInputStream;

/**
 * Test implementation for a JMS Object Message Facade.
 */
public class JmsTestObjectMessageFacade extends JmsTestMessageFacade implements JmsObjectMessageFacade {

    private byte[] object;

    public byte[] getSerializedObject() {
        return object;
    }

    public void setSerializedObject(byte[] object) {
        this.object = object;
    }

    @Override
    public JmsMsgType getMsgType() {
        return JmsMsgType.OBJECT;
    }

    @Override
    public JmsTestObjectMessageFacade copy() {
        JmsTestObjectMessageFacade copy = new JmsTestObjectMessageFacade();
        copyInto(copy);
        if (object != null) {
            copy.object = Arrays.copyOf(object, object.length);
        }

        return copy;
    }

    @Override
    public void clearBody() {
        this.object = null;
    }

    @Override
    public Serializable getObject() throws IOException, ClassNotFoundException {
        if (object == null) {
            return null;
        }

        Serializable serialized = null;

        try (ByteArrayInputStream dataIn = new ByteArrayInputStream(object);
             ClassLoadingAwareObjectInputStream objIn = new ClassLoadingAwareObjectInputStream(dataIn, null)) {

            serialized = (Serializable) objIn.readObject();
        }

        return serialized;
    }

    @Override
    public void setObject(Serializable value) throws IOException {
        byte[] serialized = null;
        if (value != null) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {

                oos.writeObject(value);
                oos.flush();
                oos.close();

                serialized = baos.toByteArray();
            }
        }

        this.object = serialized;
    }

    @Override
    public boolean hasBody() {
        return object != null && object.length > 0;
    }
}
