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
package org.apache.qpid.jms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.qpid.jms.policy.JmsDeserializationPolicy;

public class SerializationTestSupport {

    public static Object roundTripSerialize(final Object o) throws IOException, ClassNotFoundException {
        byte[] bytes = serialize(o);
        Object deserializedObject = deserialize(bytes);

        return deserializedObject;
    }

    public static byte[] serialize(final Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        oos.close();
        byte[] bytes = bos.toByteArray();
        return bytes;
    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Object deserializedObject = ois.readObject();
        ois.close();
        return deserializedObject;
    }

    public static class TestJmsDeserializationPolicy implements JmsDeserializationPolicy {

        @Override
        public JmsDeserializationPolicy copy() {
            return new TestJmsDeserializationPolicy();
        }

        @Override
        public boolean isTrustedType(JmsDestination destination, Class<?> clazz) {
            return true;
        }
    }
}
