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
package org.apache.qpid.jms.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.Vector;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.util.ClassLoadingAwareObjectInputStream.TrustedClassFilter;
import org.junit.jupiter.api.Test;

public class ClassLoadingAwareObjectInputStreamTest extends QpidJmsTestCase {

    private final TrustedClassFilter ACCEPTS_ALL_FILTER = new TrustedClassFilter() {

        @Override
        public boolean isTrusted(Class<?> clazz) {
            return true;
        }
    };

    private final TrustedClassFilter ACCEPTS_NONE_FILTER = new TrustedClassFilter() {

        @Override
        public boolean isTrusted(Class<?> clazz) {
            return false;
        }
    };

    //----- Test for serialized objects --------------------------------------//

    @Test
    public void testReadObject() throws Exception {
        // Expect to succeed
        doTestReadObject(new SimplePojo(_testMethodName), ACCEPTS_ALL_FILTER);

        // Expect to fail
        try {
            doTestReadObject(new SimplePojo(_testMethodName), ACCEPTS_NONE_FILTER);
            fail("Should have failed to read");
        } catch (ClassNotFoundException cnfe) {
            // Expected
        }
    }

    @Test
    public void testReadObjectWithAnonymousClass() throws Exception {
        AnonymousSimplePojoParent pojoParent = new AnonymousSimplePojoParent(_testMethodName);

        byte[] serialized = serializeObject(pojoParent);

        TrustedClassFilter myFilter = new TrustedClassFilter() {
            @Override
            public boolean isTrusted(Class<?> clazz) {
                return clazz.equals(AnonymousSimplePojoParent.class);
            }
        };

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
                ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter)) {

            Object obj = reader.readObject();

            assertTrue(obj instanceof AnonymousSimplePojoParent);
            assertEquals(pojoParent.getPayload(), ((AnonymousSimplePojoParent)obj).getPayload(), "Unexpected payload");
        }
    }

    @Test
    public void testReadObjectWitLocalClass() throws Exception {
        LocalSimplePojoParent pojoParent = new LocalSimplePojoParent(_testMethodName);

        byte[] serialized = serializeObject(pojoParent);

        TrustedClassFilter myFilter = new TrustedClassFilter() {
            @Override
            public boolean isTrusted(Class<?> clazz) {
                return clazz.equals(LocalSimplePojoParent.class);
            }
        };

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter)) {

            Object obj = reader.readObject();

            assertTrue(obj instanceof LocalSimplePojoParent);
            assertEquals(pojoParent.getPayload(), ((LocalSimplePojoParent)obj).getPayload(), "Unexpected payload");
        }
    }

    @Test
    public void testReadObjectByte() throws Exception {
        doTestReadObject(Byte.valueOf((byte) 255), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectShort() throws Exception {
        doTestReadObject(Short.valueOf((short) 255), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectInteger() throws Exception {
        doTestReadObject(Integer.valueOf(255), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectLong() throws Exception {
        doTestReadObject(Long.valueOf(255l), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectFloat() throws Exception {
        doTestReadObject(Float.valueOf(255.0f), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectDouble() throws Exception {
        doTestReadObject(Double.valueOf(255.0), ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectBoolean() throws Exception {
        doTestReadObject(Boolean.FALSE, ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectString() throws Exception {
        doTestReadObject(new String(_testMethodName), ACCEPTS_ALL_FILTER);
    }

    //----- Test that arrays of objects can be read --------------------------//

    @Test
    public void testReadObjectStringArray() throws Exception {
        String[] value = new String[2];

        value[0] = _testMethodName + "-1";
        value[1] = _testMethodName + "-2";

        doTestReadObject(value, ACCEPTS_ALL_FILTER);
    }

    @Test
    public void testReadObjectMultiDimensionalArray() throws Exception {
        String[][][] value = new String[2][2][1];

        value[0][0][0] = "0-0-0";
        value[0][1][0] = "0-1-0";
        value[1][0][0] = "1-0-0";
        value[1][1][0] = "1-1-0";

        doTestReadObject(value, ACCEPTS_ALL_FILTER);
    }

    //----- Test that primitive types are not filtered -----------------------//

    @Test
    public void testPrimitiveByteNotFiltered() throws Exception {
        doTestReadPrimitive((byte) 255, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveShortNotFiltered() throws Exception {
        doTestReadPrimitive((short) 255, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveIntegerNotFiltered() throws Exception {
        doTestReadPrimitive(255, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveLongNotFiltered() throws Exception {
        doTestReadPrimitive((long) 255, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveFloatNotFiltered() throws Exception {
        doTestReadPrimitive((float) 255.0, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveDoubleNotFiltered() throws Exception {
        doTestReadPrimitive(255.0, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveBooleanNotFiltered() throws Exception {
        doTestReadPrimitive(false, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitveCharNotFiltered() throws Exception {
        doTestReadPrimitive('c', ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testReadObjectStringNotFiltered() throws Exception {
        doTestReadObject(new String(_testMethodName), ACCEPTS_NONE_FILTER);
    }

    //----- Test that primitive arrays get past filters ----------------------//

    @Test
    public void testPrimitiveByteArrayNotFiltered() throws Exception {
        byte[] value = new byte[2];

        value[0] = 1;
        value[1] = 2;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveShortArrayNotFiltered() throws Exception {
        short[] value = new short[2];

        value[0] = 1;
        value[1] = 2;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveIntegerArrayNotFiltered() throws Exception {
        int[] value = new int[2];

        value[0] = 1;
        value[1] = 2;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveLongArrayNotFiltered() throws Exception {
        long[] value = new long[2];

        value[0] = 1;
        value[1] = 2;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveFloatArrayNotFiltered() throws Exception {
        float[] value = new float[2];

        value[0] = 1.1f;
        value[1] = 2.1f;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    @Test
    public void testPrimitiveDoubleArrayNotFiltered() throws Exception {
        double[] value = new double[2];

        value[0] = 1.1;
        value[1] = 2.1;

        doTestReadPrimitiveArray(value, ACCEPTS_NONE_FILTER);
    }

    //----- Tests for types that should be filtered --------------------------//

    @Test
    public void testReadObjectStringArrayFiltered() throws Exception {
        String[] value = new String[2];

        value[0] = _testMethodName + "-1";
        value[1] = _testMethodName + "-2";

        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, ACCEPTS_NONE_FILTER)) {

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }
    }

    @Test
    public void testReadObjectMixedTypeArrayGetsFiltered() throws Exception {
        Object[] value = new Object[4];

        value[0] =_testMethodName;
        value[1] = UUID.randomUUID();
        value[2] = new Vector<Object>();
        value[3] = new SimplePojo(_testMethodName);

        byte[] serialized = serializeObject(value);

        TrustedClassFilter myFilter = new TrustedClassFilter() {

            @Override
            public boolean isTrusted(Class<?> clazz) {
                return !clazz.equals(SimplePojo.class);
            }
        };

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter)) {

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }

        // Replace the filtered type and try again
        value[3] = Integer.valueOf(20);

        serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
            ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter)) {

           try {
               Object result = reader.readObject();

               assertNotNull(result);
               assertTrue(result.getClass().isArray());
           } catch (ClassNotFoundException ex) {
               fail("Should be able to read the payload.");
           }
       }
    }

    @Test
    public void testReadObjectMultiDimensionalStringArrayFiltered() throws Exception {
        String[][] value = new String[2][2];

        value[0][0] = _testMethodName + "-0-0";
        value[0][1] = _testMethodName + "-0-1";
        value[1][0] = _testMethodName + "-1-0";
        value[1][1] = _testMethodName + "-1-1";

        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, ACCEPTS_NONE_FILTER)) {

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }
    }

    @Test
    public void testReadObjectFailsWithUntrustedType() throws Exception {
        byte[] serialized = serializeObject(new SimplePojo(_testMethodName));

        TrustedClassFilter myFilter = new TrustedClassFilter() {

            @Override
            public boolean isTrusted(Class<?> clazz) {
                return !clazz.equals(SimplePojo.class);
            }
        };

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter)) {

            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }

        serialized = serializeObject(UUID.randomUUID());
        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter)) {

            try {
                reader.readObject();
            } catch (ClassNotFoundException ex) {
                fail("Should be able to read the payload.");
            }
        }
    }

    @Test
    public void testReadObjectFailsWithUnstrustedContentInTrustedType() throws Exception {
        byte[] serialized = serializeObject(new SimplePojo(UUID.randomUUID()));

        TrustedClassFilter myFilter = new TrustedClassFilter() {

            @Override
            public boolean isTrusted(Class<?> clazz) {
                return clazz.equals(SimplePojo.class);
            }
        };

        ByteArrayInputStream input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter)) {
            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {}
        }

        serialized = serializeObject(UUID.randomUUID());
        input = new ByteArrayInputStream(serialized);
        try (ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, myFilter)) {
            try {
                reader.readObject();
                fail("Should not be able to read the payload.");
            } catch (ClassNotFoundException ex) {
            }
        }
    }

    //----- Internal methods -------------------------------------------------//

    private void doTestReadObject(Object value, TrustedClassFilter filter) throws Exception {
        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, filter)) {

            Object result = reader.readObject();
            assertNotNull(result);
            assertEquals(value.getClass(), result.getClass());
            if (result.getClass().isArray()) {
                assertTrue(Arrays.deepEquals((Object[]) value, (Object[]) result));
            } else {
                assertEquals(value, result);
            }
        }
    }

    private byte[] serializeObject(Object value) throws IOException {
        byte[] result = new byte[0];

        if (value != null) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {

                oos.writeObject(value);
                oos.flush();
                oos.close();

                result = baos.toByteArray();
            }
        }

        return result;
    }

    private void doTestReadPrimitive(Object value, TrustedClassFilter filter) throws Exception {
        byte[] serialized = serializePrimitive(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, filter)) {

            Object result = null;

            if (value instanceof Byte) {
                result = reader.readByte();
            } else if (value instanceof Short) {
                result = reader.readShort();
            } else if (value instanceof Integer) {
                result = reader.readInt();
            } else if (value instanceof Long) {
                result = reader.readLong();
            } else if (value instanceof Float) {
                result = reader.readFloat();
            } else if (value instanceof Double) {
                result = reader.readDouble();
            } else if (value instanceof Boolean) {
                result = reader.readBoolean();
            } else if (value instanceof Character) {
                result = reader.readChar();
            } else {
                throw new IllegalArgumentException("unsuitable type for primitive deserialization");
            }

            assertNotNull(result);
            assertEquals(value.getClass(), result.getClass());
            assertEquals(value, result);
        }
    }

    private void doTestReadPrimitiveArray(Object value, TrustedClassFilter filter) throws Exception {
        byte[] serialized = serializeObject(value);

        try (ByteArrayInputStream input = new ByteArrayInputStream(serialized);
             ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, filter)) {

            Object result = reader.readObject();

            assertNotNull(result);
            assertEquals(value.getClass(), result.getClass());
            assertTrue(result.getClass().isArray());
            assertEquals(value.getClass().getComponentType(), result.getClass().getComponentType());
            assertTrue(result.getClass().getComponentType().isPrimitive());
        }
    }

    private byte[] serializePrimitive(Object value) throws IOException {
        byte[] result = new byte[0];

        if (value != null) {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {

                if (value instanceof Byte) {
                    oos.writeByte((byte) value);
                } else if (value instanceof Short) {
                    oos.writeShort((short) value);
                } else if (value instanceof Integer) {
                    oos.writeInt((int) value);
                } else if (value instanceof Long) {
                    oos.writeLong((long) value);
                } else if (value instanceof Float) {
                    oos.writeFloat((float) value);
                } else if (value instanceof Double) {
                    oos.writeDouble((double) value);
                } else if (value instanceof Boolean) {
                    oos.writeBoolean((boolean) value);
                } else if (value instanceof Character) {
                    oos.writeChar((char) value);
                } else {
                    throw new IllegalArgumentException("unsuitable type for primitive serialization");
                }

                oos.flush();
                oos.close();

                result = baos.toByteArray();
            }
        }

        return result;
    }
}
