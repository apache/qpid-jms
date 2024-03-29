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
package org.apache.qpid.jms.provider.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.util.URISupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test for the behavior of the FailoverUriPool
 */
public class FailoverUriPoolTest extends QpidJmsTestCase {

    private List<URI> uris;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

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

        assertTrue(pool.isEmpty());
        assertEquals(0, pool.size());
        assertNotNull(pool.getNestedOptions());
        assertTrue(pool.getNestedOptions().isEmpty());
    }

    @Test
    public void testCreateEmptyPoolFromNullUris() {
        FailoverUriPool pool = new FailoverUriPool(null, null);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, pool.isRandomize());

        assertNotNull(pool.getNestedOptions());
        assertTrue(pool.getNestedOptions().isEmpty());
    }

    @Test
    public void testCreateEmptyPoolWithURIs() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, pool.isRandomize());

        assertNotNull(pool.getNestedOptions());
        assertTrue(pool.getNestedOptions().isEmpty());
    }

    @Test
    public void testGetNextFromEmptyPool() {
        FailoverUriPool pool = new FailoverUriPool();
        assertEquals(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, pool.isRandomize());

        assertNull(pool.getNext());
    }

    @Test
    public void testGetNextFromSingleValuePool() {
        FailoverUriPool pool = new FailoverUriPool(uris.subList(0, 1), null);

        assertEquals(uris.get(0), pool.getNext());
        assertEquals(uris.get(0), pool.getNext());
        assertEquals(uris.get(0), pool.getNext());
    }

    @Test
    public void testAddUriToEmptyPool() {
        FailoverUriPool pool = new FailoverUriPool();
        assertTrue(pool.isEmpty());
        pool.add(uris.get(0));
        assertFalse(pool.isEmpty());
        assertEquals(uris.get(0), pool.getNext());
    }

    @Test
    public void testGetSetRandomize() {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        assertFalse(pool.isEmpty());
        assertFalse(pool.isRandomize());
        pool.setRandomize(true);
        assertTrue(pool.isRandomize());
        pool.setRandomize(false);
        assertFalse(pool.isRandomize());
    }

    @Test
    public void testDuplicatesNotAdded() {
        FailoverUriPool pool = new FailoverUriPool(uris, null);

        assertEquals(uris.size(), pool.size());
        pool.add(uris.get(0));
        assertEquals(uris.size(), pool.size());
        pool.add(uris.get(1));
        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testDuplicatesNotAddedByAddFirst() {
        FailoverUriPool pool = new FailoverUriPool(uris, null);

        assertEquals(uris.size(), pool.size());
        pool.addFirst(uris.get(0));
        assertEquals(uris.size(), pool.size());
        pool.addFirst(uris.get(1));
        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testDuplicatesNotAddedWhenQueryPresent() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://localhost:5671?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://localhost:5672?transport.tcpNoDelay=true"));
        assertEquals(2, pool.size());

        assertEquals(2, pool.size());
        pool.add(new URI("tcp://localhost:5672?transport.tcpNoDelay=false"));
        assertEquals(2, pool.size());
    }

    @Test
    public void testNoHostResolutionPerformedFilterDuplicatesOnly() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672"));
        assertFalse(pool.isEmpty());
        assertEquals(1, pool.size());

        assertFalse(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672"));  // Not added as it duplicates previous
        assertFalse(pool.isEmpty());
        assertEquals(1, pool.size());

        pool.add(new URI("tcp://localhost:5672"));  // Added since there is no host resolution
        assertEquals(2, pool.size());

        pool.add(new URI("tcp://localhost:5673"));  // Added since the port is different
        assertEquals(3, pool.size());

        pool.add(new URI("tcp://localhost:5672"));  // Not added as it duplicates previous
        assertEquals(3, pool.size());
    }

    @Test
    public void testDuplicatesNotAddedWhenCaseIsDifferent() throws Exception {
        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://somerandomhostname:5672"));
        assertFalse(pool.isEmpty());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SomerandomHostname:5672"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SOMERANDOMHOSTNAME:5672"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SOMERANDOMHOSTNAME2:5672"));
        assertEquals(2, pool.size());
    }

    @Test
    public void testDuplicatesNotAddedWhenCaseIsDifferentAndQueryStringPresent() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://somerandomhostname:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SomerandomHostname:5672?transport.tcpNoDelay=false"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SOMERANDOMHOSTNAME:5672?transport.tcpNoDelay=true"));
        assertEquals(1, pool.size());

        assertEquals(1, pool.size());
        pool.add(new URI("tcp://SOMERANDOMHOSTNAME2:5672?transport.tcpNoDelay=true"));
        assertEquals(2, pool.size());
    }

    @Test
    public void testAddUriToPoolRandomized() throws URISyntaxException {
        URI newUri = new URI("tcp://192.168.2." + (uris.size() + 1) + ":5672");

        FailoverUriPool pool = new FailoverUriPool(uris, null);
        pool.setRandomize(true);
        pool.add(newUri);

        URI found = null;

        for (int i = 0; i < uris.size() + 1; ++i) {
            URI next = pool.getNext();
            if (newUri.equals(next)) {
                found = next;
            }
        }

        if (found == null) {
            fail("URI added was not retrieved from the pool");
        }
    }

    @Test
    public void testAddUriToPoolNotRandomized() throws URISyntaxException {
        URI newUri = new URI("tcp://192.168.2." + (uris.size() + 1) + ":5672");

        FailoverUriPool pool = new FailoverUriPool(uris, null);
        pool.setRandomize(false);
        pool.add(newUri);

        for (int i = 0; i < uris.size(); ++i) {
            assertNotEquals(newUri, pool.getNext());
        }

        assertEquals(newUri, pool.getNext());
    }

    @Test
    public void testAddFirst() throws URISyntaxException {
        URI newUri = new URI("tcp://192.168.2." + (uris.size() + 1) + ":5672");

        FailoverUriPool pool = new FailoverUriPool(uris, null);
        pool.setRandomize(false);
        pool.addFirst(newUri);

        assertEquals(newUri, pool.getNext());

        for (int i = 0; i < uris.size(); ++i) {
            assertNotEquals(newUri, pool.getNext());
        }

        assertEquals(newUri, pool.getNext());
    }

    @Test
    public void testAddFirstHandlesNulls() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        pool.setRandomize(false);
        pool.addFirst(null);

        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testAddFirstToEmptyPool() {
        FailoverUriPool pool = new FailoverUriPool();
        assertTrue(pool.isEmpty());
        pool.addFirst(uris.get(0));
        assertFalse(pool.isEmpty());
        assertEquals(uris.get(0), pool.getNext());
    }

    @Test
    public void testAddAllHandlesNulls() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        pool.setRandomize(false);
        pool.addAll(null);

        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testAddAllHandlesEmpty() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        pool.setRandomize(false);
        pool.addAll(Collections.emptyList());

        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testAddAll() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(null, null);
        pool.setRandomize(false);

        assertEquals(0, pool.size());
        assertFalse(uris.isEmpty());

        pool.addAll(uris);

        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testRemoveURIFromPool() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        pool.setRandomize(false);

        URI removed = uris.get(0);

        pool.remove(removed);

        for (int i = 0; i < uris.size() + 1; ++i) {
            if (removed.equals(pool.getNext())) {
                fail("URI was not removed from the pool");
            }
        }
    }

    @Test
    public void testRemovedWhenQueryPresent() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://127.0.0.1:5672?transport.tcpNoDelay=true"));
        assertTrue(pool.isEmpty());
    }

    @Test
    public void testRemove() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://127.0.0.1:5672"));
        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672"));
        assertFalse(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5673"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://localhost:5673"));
        assertFalse(pool.isEmpty());
    }

    @Test
    public void testRemoveHostIgnoresCaseInHostname() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://somerandomhostname:5672"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://SOMERANDOMHOSTNAME:5672"));
        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://somerandomhostname:5672"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://somerandomhostname:5673"));
        assertFalse(pool.isEmpty());
    }

    @Test
    public void testRemoveWhenQueryPresentIgnoresCaseInHostname() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://somerandomhostname:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://SOMERANDOMHOSTNAME:5672?transport.tcpNoDelay=true"));
        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://somerandomhostname:5672?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());
        pool.remove(new URI("tcp://somerandomhostname:5673?transport.tcpNoDelay=true"));
        assertFalse(pool.isEmpty());
    }

    @Test
    public void testConnectedShufflesWhenRandomizing() {
        assertConnectedEffectOnPool(true, true);
    }

    @Test
    public void testConnectedDoesNotShufflesWhenNoRandomizing() {
        assertConnectedEffectOnPool(false, false);
    }

    private void assertConnectedEffectOnPool(boolean randomize, boolean shouldShuffle) {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        pool.setRandomize(randomize);

        List<URI> current = new ArrayList<URI>();
        List<URI> previous = new ArrayList<URI>();

        boolean shuffled = false;

        for (int i = 0; i < 10; ++i) {

            for (int j = 0; j < uris.size(); ++j) {
                current.add(pool.getNext());
            }

            pool.connected();

            if (!previous.isEmpty() && !previous.equals(current)) {
                shuffled = true;
                break;
            }

            previous.clear();
            previous.addAll(current);
            current.clear();
        }

        if (shouldShuffle) {
            assertTrue(shuffled, "URIs did not get randomized");
        } else {
            assertFalse(shuffled, "URIs should not get randomized");
        }
    }

    @Test
    public void testAddOrRemoveNullHasNoAffect() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        assertEquals(uris.size(), pool.size());

        pool.add(null);
        assertEquals(uris.size(), pool.size());
        pool.remove(null);
        assertEquals(uris.size(), pool.size());
    }

    @Test
    public void testNestedOptionsAreApplied() throws URISyntaxException {
        Map<String, String> nested = new HashMap<String, String>();

        nested.put("transport.tcpNoDelay", "true");
        nested.put("transport.tcpKeepAlive", "false");

        FailoverUriPool pool = new FailoverUriPool(uris, nested);
        assertNotNull(pool.getNestedOptions());
        assertFalse(pool.getNestedOptions().isEmpty());

        for (int i = 0; i < uris.size(); ++i) {
            URI next = pool.getNext();
            assertNotNull(next);

            String query = next.getQuery();
            assertNotNull(query);
            assertFalse(query.isEmpty());

            Map<String, String> options = URISupport.parseParameters(next);
            assertFalse(options.isEmpty());

            assertTrue(options.containsKey("transport.tcpNoDelay"));
            assertTrue(options.containsKey("transport.tcpKeepAlive"));

            assertEquals("true", options.get("transport.tcpNoDelay"));
            assertEquals("false", options.get("transport.tcpKeepAlive"));
        }
    }

    @Test
    public void testRemoveURIWhenNestedOptionsSet() throws URISyntaxException {
        Map<String, String> nested = new HashMap<String, String>();

        nested.put("transport.tcpNoDelay", "true");
        nested.put("transport.tcpKeepAlive", "false");

        FailoverUriPool pool = new FailoverUriPool(uris, nested);
        assertNotNull(pool.getNestedOptions());
        assertFalse(pool.getNestedOptions().isEmpty());

        for (int i = 0; i < uris.size(); ++i) {
            assertTrue(pool.remove(uris.get(i)));
            assertEquals(uris.size() - (i + 1), pool.size());
        }
    }

    @Test
    public void testAddURIWhenNestedOptionsSet() throws URISyntaxException {
        Map<String, String> nested = new HashMap<String, String>();

        nested.put("transport.tcpNoDelay", "true");
        nested.put("transport.tcpKeepAlive", "false");

        FailoverUriPool pool = new FailoverUriPool(null, nested);
        assertNotNull(pool.getNestedOptions());
        assertFalse(pool.getNestedOptions().isEmpty());

        assertTrue(pool.isEmpty());
        pool.add(uris.get(0));
        assertFalse(pool.isEmpty());
        assertEquals(uris.get(0).getHost(), pool.getNext().getHost());
    }

    @Test
    public void testAddFirstURIWhenNestedOptionsSet() throws URISyntaxException {
        Map<String, String> nested = new HashMap<String, String>();

        nested.put("transport.tcpNoDelay", "true");
        nested.put("transport.tcpKeepAlive", "false");

        FailoverUriPool pool = new FailoverUriPool(null, nested);
        assertNotNull(pool.getNestedOptions());
        assertFalse(pool.getNestedOptions().isEmpty());

        assertTrue(pool.isEmpty());
        pool.addFirst(uris.get(0));
        assertFalse(pool.isEmpty());
        assertEquals(uris.get(0).getHost(), pool.getNext().getHost());
    }

    @Test
    public void testRemoveAll() throws URISyntaxException {
        FailoverUriPool pool = new FailoverUriPool(uris, null);
        assertEquals(uris.size(), pool.size());

        pool.removeAll();
        assertTrue(pool.isEmpty());
        assertEquals(0, pool.size());

        pool.removeAll();
    }

    @Test
    public void testLoopBackAndLocalHostAllowedInSamePool() throws URISyntaxException {
        assumeTrue(checkLocalHostResolvesToLoopbackIP());

        FailoverUriPool pool = new FailoverUriPool();

        assertTrue(pool.isEmpty());
        pool.add(new URI("tcp://127.0.0.1:5672"));
        assertFalse(pool.isEmpty());
        assertEquals(1, pool.size());
        pool.add(new URI("tcp://localhost:5672"));
        assertEquals(2, pool.size());
    }

    private boolean checkLocalHostResolvesToLoopbackIP() {
        try {
            InetAddress localhost = InetAddress.getByName("localhost");
            InetAddress loopback = InetAddress.getLoopbackAddress();

            return localhost.equals(loopback);
        } catch (Exception failsOnError) {
            return false;
        }
    }
}
