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
package org.apache.qpid.jms.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.UUID;
import java.util.Vector;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.util.ClassLoadingAwareObjectInputStream;
import org.apache.qpid.jms.util.ClassLoadingAwareObjectInputStream.TrustedClassFilter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsDefaultDeserializationPolicyTest {

    private static final Logger LOG = LoggerFactory.getLogger(JmsDefaultDeserializationPolicyTest.class);

    @Test
    public void testIsTrustedType() {
        JmsDestination destination = new JmsQueue("test-queue");
        JmsDefaultDeserializationPolicy policy = new JmsDefaultDeserializationPolicy();

        assertTrue(policy.isTrustedType(destination, null));
        assertTrue(policy.isTrustedType(destination, UUID.class));
        assertTrue(policy.isTrustedType(destination, String.class));
        assertTrue(policy.isTrustedType(destination, Boolean.class));
        assertTrue(policy.isTrustedType(destination, Double.class));
        assertTrue(policy.isTrustedType(destination, Object.class));

        // Only types in lang
        policy.setWhiteList("java.lang");

        assertTrue(policy.isTrustedType(destination, null));
        assertFalse(policy.isTrustedType(destination, UUID.class));
        assertTrue(policy.isTrustedType(destination, String.class));
        assertTrue(policy.isTrustedType(destination, Boolean.class));
        assertFalse(policy.isTrustedType(destination, getClass()));

        // Entry must be complete package name prefix to match
        // i.e while "java.n" is a prefix of "java.net", this
        // wont match the socket class below.
        policy.setWhiteList("java.n");
        assertFalse(policy.isTrustedType(destination, UUID.class));
        assertFalse(policy.isTrustedType(destination, String.class));
        assertFalse(policy.isTrustedType(destination, java.net.Socket.class));

        // add a non-core package
        policy.setWhiteList("java.lang,org.apache.qpid.jms");

        assertFalse(policy.isTrustedType(destination, UUID.class));
        assertTrue(policy.isTrustedType(destination, String.class));
        assertTrue(policy.isTrustedType(destination, getClass()));

        // Try with a class-specific entry
        policy.setWhiteList("java.lang.Integer");

        assertTrue(policy.isTrustedType(destination, Integer.class));
        assertFalse(policy.isTrustedType(destination, Boolean.class));

        // Verify blacklist overrides whitelist
        policy.setWhiteList("java.lang.Integer");
        policy.setBlackList("java.lang.Integer");

        assertFalse(policy.isTrustedType(destination, Integer.class));

        // Verify blacklist entry prefix overrides whitelist
        policy.setWhiteList("java.lang.Integer");
        policy.setBlackList("java.lang");

        assertFalse(policy.isTrustedType(destination, Integer.class));

        // Verify blacklist catch-all overrides whitelist
        policy.setWhiteList("java.lang.Integer");
        policy.setBlackList("*");

        assertFalse(policy.isTrustedType(destination, Integer.class));
    }

    @Test
    public void testHashCode() {
        JmsDeserializationPolicy policy1 = new JmsDefaultDeserializationPolicy();
        JmsDeserializationPolicy policy2 = new JmsDefaultDeserializationPolicy();

        assertTrue(policy1.hashCode() != 0);
        assertEquals(policy1.hashCode(), policy2.hashCode());
        assertEquals(policy2.hashCode(), policy1.hashCode());

        ((JmsDefaultDeserializationPolicy) policy1).setWhiteList("java.util");

        assertFalse(policy1.hashCode() == policy2.hashCode());
        assertFalse(policy2.hashCode() == policy1.hashCode());

        ((JmsDefaultDeserializationPolicy) policy2).setWhiteList("java.util");

        assertTrue(policy1.hashCode() == policy2.hashCode());
        assertTrue(policy2.hashCode() == policy1.hashCode());

        ((JmsDefaultDeserializationPolicy) policy1).setBlackList("java.util");

        assertFalse(policy1.hashCode() == policy2.hashCode());
        assertFalse(policy2.hashCode() == policy1.hashCode());

        ((JmsDefaultDeserializationPolicy) policy2).setBlackList("java.util");

        assertTrue(policy1.hashCode() == policy2.hashCode());
        assertTrue(policy2.hashCode() == policy1.hashCode());
    }

    @Test
    public void testEqualsObject() {
        JmsDefaultDeserializationPolicy policy1 = new JmsDefaultDeserializationPolicy();
        JmsDefaultDeserializationPolicy policy2 = new JmsDefaultDeserializationPolicy();

        assertTrue(policy1.equals(policy1));
        assertTrue(policy1.equals(policy2));
        assertTrue(policy2.equals(policy1));

        policy1.setWhiteList("java.util");

        assertFalse(policy1.equals(policy2));
        assertFalse(policy2.equals(policy1));

        assertFalse(policy1.equals(null));
        assertFalse(policy1.equals(""));
        assertFalse(policy1.equals(this));

        policy2.setWhiteList("java.util");
        assertTrue(policy1.equals(policy2));

        policy1.setBlackList("java.util");

        assertFalse(policy1.equals(policy2));
        assertFalse(policy2.equals(policy1));

        policy2.setBlackList("java.util");
        assertTrue(policy1.equals(policy2));
        assertTrue(policy2.equals(policy1));
    }

    @Test
    public void testJmsDefaultDeserializationPolicy() {
        JmsDefaultDeserializationPolicy policy = new JmsDefaultDeserializationPolicy();

        assertFalse(policy.getWhiteList().isEmpty());
        assertTrue(policy.getBlackList().isEmpty());
    }

    @Test
    public void testJmsDefaultDeserializationPolicyCopyCtor() {
        JmsDefaultDeserializationPolicy policy = new JmsDefaultDeserializationPolicy();

        policy.setWhiteList("a.b.c");
        policy.setBlackList("d.e.f");

        JmsDefaultDeserializationPolicy copy = new JmsDefaultDeserializationPolicy(policy);

        assertEquals("a.b.c", copy.getWhiteList());
        assertEquals("d.e.f", copy.getBlackList());
    }

    @Test
    public void testJmsDefaultDeserializationPolicyCopy() {
        JmsDefaultDeserializationPolicy policy = new JmsDefaultDeserializationPolicy();

        policy.setWhiteList("a.b.c");
        policy.setBlackList("d.e.f");

        JmsDefaultDeserializationPolicy copy = (JmsDefaultDeserializationPolicy) policy.copy();

        assertEquals("a.b.c", copy.getWhiteList());
        assertEquals("d.e.f", copy.getBlackList());
    }

    @Test
    public void testSetWhiteList() {
        JmsDefaultDeserializationPolicy policy = new JmsDefaultDeserializationPolicy();
        assertNotNull(policy.getWhiteList());

        policy.setWhiteList(null);
        assertNotNull(policy.getWhiteList());
        assertTrue(policy.getWhiteList().isEmpty());

        policy.setWhiteList("");
        assertNotNull(policy.getWhiteList());
        assertTrue(policy.getWhiteList().isEmpty());

        policy.setWhiteList("*");
        assertNotNull(policy.getWhiteList());
        assertFalse(policy.getWhiteList().isEmpty());

        policy.setWhiteList("a,b,c");
        assertNotNull(policy.getWhiteList());
        assertFalse(policy.getWhiteList().isEmpty());
        assertEquals("a,b,c", policy.getWhiteList());
    }

    @Test
    public void testSetBlackList() {
        JmsDefaultDeserializationPolicy policy = new JmsDefaultDeserializationPolicy();
        assertNotNull(policy.getBlackList());

        policy.setBlackList(null);
        assertNotNull(policy.getBlackList());
        assertTrue(policy.getBlackList().isEmpty());

        policy.setBlackList("");
        assertNotNull(policy.getBlackList());
        assertTrue(policy.getBlackList().isEmpty());

        policy.setBlackList("*");
        assertNotNull(policy.getBlackList());
        assertFalse(policy.getBlackList().isEmpty());

        policy.setBlackList("a,b,c");
        assertNotNull(policy.getBlackList());
        assertFalse(policy.getBlackList().isEmpty());
        assertEquals("a,b,c", policy.getBlackList());
    }

    @Test
    public void testDeserializeVectorUsingPolicy() throws Exception {
        Vector<Object> vector = new Vector<Object>();
        vector.add("pi");
        vector.add(Integer.valueOf(314159));
        vector.add(new Vector<String>());
        vector.add(Boolean.FALSE);

        final JmsDefaultDeserializationPolicy policy = new JmsDefaultDeserializationPolicy();
        ByteArrayInputStream input = new ByteArrayInputStream(serializeObject(vector));
        TrustedClassFilter filter = new TrustedClassFilter() {

            @Override
            public boolean isTrusted(Class<?> clazz) {
                LOG.trace("Check for trust status of class: {}", clazz.getName());
                return policy.isTrustedType(new JmsQueue(), clazz);
            }
        };

        ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, filter);

        Object result = null;
        try {
            result = reader.readObject();
        } catch (Exception ex) {
            fail("Should no throw any errors");
        } finally {
            reader.close();
        }

        assertNotNull(result);
        assertTrue(result instanceof Vector);
        assertEquals(4, ((Vector<?>) result).size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDeserializeHashMapUsingPolicy() throws Exception {
        HashMap<Object, Object> map = new HashMap<Object, Object>();

        map.put("a", "Value");
        map.put("b", Integer.valueOf(1));
        map.put("c", new Vector<Object>());
        map.put("d", Boolean.FALSE);

        final JmsDefaultDeserializationPolicy policy = new JmsDefaultDeserializationPolicy();
        ByteArrayInputStream input = new ByteArrayInputStream(serializeObject(map));
        TrustedClassFilter filter = new TrustedClassFilter() {

            @Override
            public boolean isTrusted(Class<?> clazz) {
                LOG.trace("Check for trust status of class: {}", clazz.getName());
                return policy.isTrustedType(new JmsQueue(), clazz);
            }
        };

        ClassLoadingAwareObjectInputStream reader = new ClassLoadingAwareObjectInputStream(input, filter);

        Object result = null;
        try {
            result = reader.readObject();
        } catch (Exception ex) {
            fail("Should no throw any errors");
        } finally {
            reader.close();
        }

        assertNotNull(result);
        assertTrue(result instanceof HashMap);

        map = (HashMap<Object, Object>) result;

        assertEquals(4, map.size());

        assertEquals("Value", map.get("a"));
        assertEquals(Integer.valueOf(1), map.get("b"));
        assertEquals(new Vector<Object>(), map.get("c"));
        assertEquals(Boolean.FALSE, map.get("d"));
    }

    //----- Internal methods -------------------------------------------------//

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
}
