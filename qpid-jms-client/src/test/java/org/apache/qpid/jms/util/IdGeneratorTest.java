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
package org.apache.qpid.jms.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
    public void testGetHostName() {
        assertNotNull(IdGenerator.getHostName());
    }

    @Test
    public void testGenerateSanitizedId() {
        IdGenerator generator = new IdGenerator("A:B.C");
        assertNotNull(generator.generateSanitizedId());
        String newId = generator.generateSanitizedId();
        assertFalse(newId.contains(":"));
        assertFalse(newId.contains("."));
    }

    @Test
    public void testGetSequenceFromId() {
        assertNotNull(IdGenerator.getSequenceFromId(generator.generateId()));
    }

    @Test
    public void testGetSequenceFromNullId() {
        assertEquals(-1, IdGenerator.getSequenceFromId(null));
    }

    @Test
    public void testGetSequenceFromNonConformingId() {
        assertEquals(-1, IdGenerator.getSequenceFromId("SomeIdValue"));
    }

    @Test
    public void testGetSequenceFromNonConformingId2() {
        assertEquals(-1, IdGenerator.getSequenceFromId("SomeIdValue:"));
    }

    @Test
    public void testGetSeedFromId() {
        assertNotNull(IdGenerator.getSeedFromId(generator.generateId()));
    }

    @Test
    public void testGetSeedFromNullId() {
        assertNull(IdGenerator.getSeedFromId(null));
    }

    @Test
    public void testGetSeedFromNonConformingId() {
        assertEquals("SomeIdValue", IdGenerator.getSeedFromId("SomeIdValue"));
    }

    @Test
    public void testGetSeedFromNonConformingId2() {
        assertEquals("SomeIdValue:", IdGenerator.getSeedFromId("SomeIdValue:"));
    }

    @Test
    public void testCompareIds() {
        IdGenerator gen = new IdGenerator();

        String id1 = generator.generateId();
        String id2 = generator.generateId();
        String id3 = generator.generateId();

        assertEquals(0, IdGenerator.compare(id1, id1));
        assertEquals(1, IdGenerator.compare(id2, id1));
        assertEquals(-1, IdGenerator.compare(id2, id3));
        assertEquals(-1, IdGenerator.compare(id2, null));
        assertEquals(-1, IdGenerator.compare(null, id3));

        String idg1 = gen.generateId();
        assertEquals(1, IdGenerator.compare(idg1, id3));
    }
}
