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
package org.apache.qpid.jms.provider.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 * Test for the behavior of the FailoverUriPool
 */
public class FailoverUriPoolTest {

    private List<URI> uris;

    @Before
    public void setUp() throws Exception {
        uris = new ArrayList<URI>();

        uris.add(new URI("tcp://192.168.2.1:5672"));
        uris.add(new URI("tcp://192.168.2.2:5672"));
        uris.add(new URI("tcp://192.168.2.3:5672"));
        uris.add(new URI("tcp://192.168.2.4:5672"));
    }

    @Test
    public void testCreateEmptyPool() {
        FailoverUriPool pool = new FailoverUriPool();
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, pool.isRandomize());
    }

    @Test
    public void testCreateEmptyPoolWithURIs() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(uris.toArray(new URI[0]), null);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, pool.isRandomize());

        assertNotNull(pool.getNestedOptions());
        assertTrue(pool.getNestedOptions().isEmpty());
    }
}
