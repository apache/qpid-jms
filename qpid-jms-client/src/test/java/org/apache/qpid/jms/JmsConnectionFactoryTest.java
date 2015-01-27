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

import static org.apache.qpid.jms.SerializationTestSupport.roundTripSerialize;
import static org.apache.qpid.jms.SerializationTestSupport.serialize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import javax.jms.IllegalStateException;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

public class JmsConnectionFactoryTest extends QpidJmsTestCase {

    private static String CLIENT_ID_PROP = "clientID";
    private static String QUEUE_PREFIX_PROP = "queuePrefix";

    @Test
    public void testCreateConnectionWithoutUriThrowsJMSISE() throws Exception {
        JmsConnectionFactory cf = new JmsConnectionFactory();
        try {
            cf.createConnection();
            fail("Should have thrown exception");
        } catch (IllegalStateException jmsise){
            // expected
        }
    }

    @Test
    public void testSetProperties() throws Exception {
        String clientID = getTestName();
        String queuePrefix = "q:";
        String jmsOptionPrefix = "jms.";
        String baseUri = "amqp://localhost:1234";
        String uri = baseUri + "?" + jmsOptionPrefix + CLIENT_ID_PROP + "=" + clientID;

        // Create a connection factory object
        JmsConnectionFactory cf = new JmsConnectionFactory();

        // Verify the outcome conditions have not been met already
        assertNotEquals("value should not match yet", clientID, cf.getClientID());
        assertNotEquals("value should not match yet", queuePrefix, cf.getQueuePrefix());
        assertNotEquals("value should not match yet", baseUri, cf.getRemoteURI());

        // Set the properties
        Map<String, String> props = new HashMap<String, String>();
        // Add the URI property, itself containing a property option in its query
        props.put("remoteURI", uri);
        // Add another property directly
        props.put("queuePrefix", queuePrefix);
        cf.setProperties(props);

        // Verify the clientID property option from the URI was applied.
        assertEquals("uri property query option not applied as expected", clientID, cf.getClientID());
        // Verify the direct property was applied
        assertEquals("direct property not applied as expected", queuePrefix, cf.getQueuePrefix());
        // Verify the URI was filtered to remove the applied options
        assertEquals("URI was filtered to remove options that were applied", baseUri, cf.getRemoteURI());
    }

    @Test
    public void testGetProperties() throws Exception {
        String clientID = getTestName();
        String queuePrefix = "q:";
        String jmsOptionPrefix = "jms.";
        String clientIDprop = "clientID";
        String baseUri = "amqp://localhost:1234";
        String uri = baseUri + "?" + jmsOptionPrefix + clientIDprop + "=" + clientID;

        JmsConnectionFactory cf = new JmsConnectionFactory();

        // Set the URI property, itself containing a property option in its query
        cf.setRemoteURI(uri);
        // Set another property directly
        cf.setQueuePrefix(queuePrefix);

        // Get the properties
        Map<String, String> props = cf.getProperties();

        // Verify the clientID property option from the URI was applied.
        assertTrue(CLIENT_ID_PROP + " property not found", props.containsKey(CLIENT_ID_PROP));
        assertEquals("clientID uri property query option not applied as expected", clientID, props.get(CLIENT_ID_PROP));
        assertTrue(QUEUE_PREFIX_PROP + " property not found", props.containsKey(QUEUE_PREFIX_PROP));
        assertEquals("queue prefix property not applied as expected", queuePrefix, props.get(QUEUE_PREFIX_PROP));
    }

    @Test
    public void testSerializeThenDeserialize() throws Exception {
        String uri = "amqp://localhost:1234";

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull("Null object returned", roundTripped);
        assertEquals("Unexpected type", JmsConnectionFactory.class, roundTripped.getClass());
        assertEquals("Unexpected uri", uri, ((JmsConnectionFactory)roundTripped).getRemoteURI());

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertEquals("Properties were not equal", props, props2);
    }

    /**
     * The prefetch policy is maintained in a child-object, which we extract the properties from
     * when serializing the factory. Ensure this functions by doing a round trip on a factory
     * configured with some new prefetch configuration via the URI.
     */
    @Test
    public void testSerializeThenDeserializeMaintainsPrefetchPolicy() throws Exception {
        String topicPrefetchValue = "17";
        String topicPrefetchKey = "prefetchPolicy.topicPrefetch";
        String uri = "amqp://localhost:1234?jms." + topicPrefetchKey + "=" + topicPrefetchValue;

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        assertTrue("Props dont contain expected prefetch policy change", props.containsKey(topicPrefetchKey));
        assertEquals("Unexpected value", topicPrefetchValue, props.get(topicPrefetchKey));

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull("Null object returned", roundTripped);
        assertEquals("Unexpected type", JmsConnectionFactory.class, roundTripped.getClass());

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertTrue("Props dont contain expected prefetch policy change", props2.containsKey(topicPrefetchKey));
        assertEquals("Unexpected value", topicPrefetchValue, props2.get(topicPrefetchKey));

        assertEquals("Properties were not equal", props, props2);
    }

    @Test
    public void testSerializeTwoConnectionFactories() throws Exception {
        String uri = "amqp://localhost:1234";

        JmsConnectionFactory cf1 = new JmsConnectionFactory(uri);
        JmsConnectionFactory cf2 = new JmsConnectionFactory(uri);

        byte[] bytes1 = serialize(cf1);
        byte[] bytes2 = serialize(cf2);

        assertArrayEquals(bytes1, bytes2);
    }

    @Test
    public void testSerializeTwoDifferentConnectionFactories() throws Exception {
        JmsConnectionFactory cf1 = new JmsConnectionFactory("amqp://localhost:1234");
        JmsConnectionFactory cf2 = new JmsConnectionFactory("amqp://localhost:5678");

        byte[] bytes1 = serialize(cf1);
        byte[] bytes2 = serialize(cf2);

        try {
            assertArrayEquals(bytes1, bytes2);
            fail("Expected arrays to differ");
        } catch (AssertionError ae) {
            // Expected, pass
        }
    }
}
