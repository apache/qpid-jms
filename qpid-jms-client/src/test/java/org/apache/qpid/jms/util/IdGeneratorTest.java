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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

public class IdGeneratorTest {

    IdGenerator generator;

    @Before
    public void setUp() {
        generator = new IdGenerator();
    }

    @Test
    public void testSanitizeHostName() throws Exception {
        assertEquals("somehost.lan", IdGenerator.sanitizeHostName("somehost.lan"));
        // include a UTF-8 char in the text \u0E08 is a Thai elephant
        assertEquals("otherhost.lan", IdGenerator.sanitizeHostName("other\u0E08host.lan"));
    }

    @Test
    public void testDefaultPrefix() {
        String generated = generator.generateId();
        assertTrue(generated.startsWith(IdGenerator.DEFAULT_PREFIX));
        assertFalse(generated.substring(IdGenerator.DEFAULT_PREFIX.length()).startsWith(":"));
    }

    @Test
    public void testNonDefaultPrefix() {
        generator = new IdGenerator("TEST-");
        String generated = generator.generateId();
        assertFalse(generated.startsWith(IdGenerator.DEFAULT_PREFIX));
        assertFalse(generated.substring("TEST-".length()).startsWith(":"));
    }

    @Test
    public void testIdIndexIncrements() throws Exception {

        final int COUNT = 5;

        ArrayList<String> ids = new ArrayList<String>(COUNT);
        ArrayList<Integer> sequences = new ArrayList<Integer>();

        for (int i = 0; i < COUNT; ++i) {
            ids.add(generator.generateId());
        }

        for (String id : ids) {
            String[] components = id.split(":");
            sequences.add(Integer.parseInt(components[components.length - 1]));
        }

        Integer lastValue = null;
        for (Integer sequence : sequences) {
            if (lastValue != null) {
                assertTrue(sequence > lastValue);
            }
            lastValue = sequence;
        }
    }
}
