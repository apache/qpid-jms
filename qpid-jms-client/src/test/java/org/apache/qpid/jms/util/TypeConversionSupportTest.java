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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.UUID;

import org.junit.Test;

public class TypeConversionSupportTest {

    @Test
    public void testConversionStringToUUID() {
        String result = (String) TypeConversionSupport.convert("42", UUID.class);
        assertNull(result);
    }

    @Test
    public void testConversionUUIDToString() {
        String result = (String) TypeConversionSupport.convert(UUID.randomUUID(), String.class);
        assertNull(result);
    }

    //----- String conversion from some other type ---------------------------//

    @Test
    public void testConversionStringToString() {
        String result = (String) TypeConversionSupport.convert("42", String.class);
        assertNotNull(result);
        assertEquals("42", result);
    }

    @Test
    public void testConversionByteToString() {
        String result = (String) TypeConversionSupport.convert((byte) 42, String.class);
        assertNotNull(result);
        assertEquals("42", result);
    }

    @Test
    public void testConversionShortToString() {
        String result = (String) TypeConversionSupport.convert((short) 42, String.class);
        assertNotNull(result);
        assertEquals("42", result);
    }

    @Test
    public void testConversionIntToString() {
        String result = (String) TypeConversionSupport.convert(42, String.class);
        assertNotNull(result);
        assertEquals("42", result);
    }

    @Test
    public void testConversionLongToString() {
        String result = (String) TypeConversionSupport.convert((long) 42, String.class);
        assertNotNull(result);
        assertEquals("42", result);
    }

    @Test
    public void testConversionFloatToString() {
        String result = (String) TypeConversionSupport.convert(42.0f, String.class);
        assertNotNull(result);
        assertEquals("42.0", result);
    }

    @Test
    public void testConversionDoubleToString() {
        String result = (String) TypeConversionSupport.convert(42.0, String.class);
        assertNotNull(result);
        assertEquals("42.0", result);
    }

    //----- Byte conversion from some other type ---------------------------//

    @Test
    public void testConversionStringToByte() {
        byte result = (byte) TypeConversionSupport.convert("42", Byte.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionStringToPrimitiveByte() {
        byte result = (byte) TypeConversionSupport.convert("42", byte.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionByteToByte() {
        byte result = (byte) TypeConversionSupport.convert((byte) 42, Byte.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    //----- Short conversion from some other type ----------------------------//

    @Test
    public void testConversionStringToShort() {
        short result = (short) TypeConversionSupport.convert("42", Short.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionStringToPrimitiveShort() {
        short result = (short) TypeConversionSupport.convert("42", short.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionByteToShort() {
        short result = (short) TypeConversionSupport.convert((byte) 42, Short.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionShortToShort() {
        short result = (short) TypeConversionSupport.convert((short) 42, Short.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    //----- Integer conversion from some other type --------------------------//

    @Test
    public void testConversionStringToInt() {
        int result = (int) TypeConversionSupport.convert("42", Integer.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionStringToPrimitiveInt() {
        int result = (int) TypeConversionSupport.convert("42", int.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionByteToInt() {
        int result = (int) TypeConversionSupport.convert((byte) 42, Integer.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionShortToInt() {
        int result = (int) TypeConversionSupport.convert((short) 42, Integer.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionIntToInt() {
        int result = (int) TypeConversionSupport.convert(42, Integer.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    //----- Long conversion from some other type --------------------------//

    @Test
    public void testConversionStringToLong() {
        long result = (long) TypeConversionSupport.convert("42", Long.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionStringToPrimitiveLong() {
        long result = (long) TypeConversionSupport.convert("42", long.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionByteToLong() {
        long result = (long) TypeConversionSupport.convert((byte) 42, Long.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionShortToLong() {
        long result = (long) TypeConversionSupport.convert((short) 42, Long.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionIntToLong() {
        long result = (long) TypeConversionSupport.convert(42, Long.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionLongToLong() {
        long result = (long) TypeConversionSupport.convert((long) 42, Long.class);
        assertNotNull(result);
        assertEquals(42, result);
    }

    @Test
    public void testConversionDateToLong() {
        Date now = new Date(System.currentTimeMillis());
        long result = (long) TypeConversionSupport.convert(now, Long.class);
        assertNotNull(result);
        assertEquals(now.getTime(), result);
    }

    //----- Float conversion from some other type --------------------------//

    @Test
    public void testConversionStringToFloat() {
        float result = (float) TypeConversionSupport.convert("42.0", Float.class);
        assertNotNull(result);
        assertEquals(42.0, result, 0.5f);
    }

    @Test
    public void testConversionStringToPrimitiveFloat() {
        float result = (float) TypeConversionSupport.convert("42.0", float.class);
        assertNotNull(result);
        assertEquals(42.0, result, 0.5f);
    }

    @Test
    public void testConversionFloatToFloat() {
        float result = (float) TypeConversionSupport.convert(42.0f, Float.class);
        assertNotNull(result);
        assertEquals(42f, result, 0.5f);
    }

    //----- Float conversion from some other type --------------------------//

    @Test
    public void testConversionStringToDouble() {
        double result = (double) TypeConversionSupport.convert("42.0", Double.class);
        assertNotNull(result);
        assertEquals(42.0, result, 0.5f);
    }

    @Test
    public void testConversionStringToPrimitiveDouble() {
        double result = (double) TypeConversionSupport.convert("42.0", double.class);
        assertNotNull(result);
        assertEquals(42.0, result, 0.5f);
    }

    @Test
    public void testConversionFloatToDouble() {
        double result = (double) TypeConversionSupport.convert(42.0f, Double.class);
        assertNotNull(result);
        assertEquals(42, result, 0.5);
    }

    @Test
    public void testConversionDoubleToDouble() {
        double result = (double) TypeConversionSupport.convert(42.0, Double.class);
        assertNotNull(result);
        assertEquals(42, result, 0.5);
    }

    //----- Boolean conversion from some other type --------------------------//

    @Test
    public void testConversionStringToBoolean() {
        boolean result = (boolean) TypeConversionSupport.convert("true", Boolean.class);
        assertNotNull(result);
        assertTrue(result);
    }

    @Test
    public void testConversionStringToPrimitiveBoolean() {
        boolean result = (boolean) TypeConversionSupport.convert("true", boolean.class);
        assertNotNull(result);
        assertTrue(result);
    }

    @Test
    public void testConversionBooleanToBoolean() {
        boolean result = (boolean) TypeConversionSupport.convert(true, Boolean.class);
        assertNotNull(result);
        assertTrue(result);
    }
}
