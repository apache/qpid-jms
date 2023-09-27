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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;

import org.apache.qpid.jms.policy.JmsDefaultDeserializationPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPresettlePolicy;
import org.apache.qpid.jms.policy.JmsDefaultRedeliveryPolicy;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsConnectionFactoryTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnectionFactoryTest.class);

    private static final String CLIENT_ID_PROP = "clientID";
    private static final String QUEUE_PREFIX_PROP = "queuePrefix";
    private static final String TOPIC_PREFIX_PROP = "topicPrefix";
    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";

    @Test
    public void testConnectionFactoryCreate() {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        assertNull(factory.getUsername());
        assertNull(factory.getPassword());
        assertNotNull(factory.getRemoteURI());
    }

    @Test
    public void testConnectionFactoryCreateUsernameAndPassword() {
        JmsConnectionFactory factory = new JmsConnectionFactory(USER, PASSWORD);
        assertNotNull(factory.getUsername());
        assertNotNull(factory.getPassword());
        assertEquals(USER, factory.getUsername());
        assertEquals(PASSWORD, factory.getPassword());
    }

    @Test
    public void testConnectionFactoryOptionsAreAppliedToConnection() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(USER, PASSWORD, "mock://localhost");

        factory.setTopicPrefix(TOPIC_PREFIX_PROP);
        factory.setQueuePrefix(QUEUE_PREFIX_PROP);
        factory.setClientID(CLIENT_ID_PROP);

        factory.setForceSyncSend(!factory.isForceSyncSend());
        factory.setForceAsyncSend(!factory.isForceAsyncSend());
        factory.setLocalMessagePriority(!factory.isLocalMessagePriority());
        factory.setForceAsyncAcks(!factory.isForceAsyncAcks());
        factory.setConnectTimeout(TimeUnit.SECONDS.toMillis(30));
        factory.setCloseTimeout(TimeUnit.SECONDS.toMillis(45));
        factory.setUseDaemonThread(true);

        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);

        assertEquals(USER, connection.getUsername());
        assertEquals(PASSWORD, connection.getPassword());
        assertEquals(CLIENT_ID_PROP, connection.getClientID());
        assertEquals(TOPIC_PREFIX_PROP, connection.getTopicPrefix());
        assertEquals(QUEUE_PREFIX_PROP, connection.getQueuePrefix());

        assertEquals(factory.isForceSyncSend(), connection.isForceSyncSend());
        assertEquals(factory.isForceAsyncSend(), connection.isForceAsyncSend());
        assertEquals(factory.isLocalMessagePriority(), connection.isLocalMessagePriority());
        assertEquals(factory.isForceAsyncAcks(), connection.isForceAsyncAcks());
        assertEquals(factory.isUseDaemonThread(), connection.isUseDaemonThread());

        assertEquals(TimeUnit.SECONDS.toMillis(30), connection.getConnectTimeout());
        assertEquals(TimeUnit.SECONDS.toMillis(45), connection.getCloseTimeout());

        connection.close();
    }

    @Test
    public void testConnectionFactoryPrefetchPolicyIsAppliedToConnection() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(USER, PASSWORD, "mock://localhost");

        JmsDefaultPrefetchPolicy prefetchPolicy = (JmsDefaultPrefetchPolicy) factory.getPrefetchPolicy();

        assertFalse(prefetchPolicy.getQueuePrefetch() == 1);

        ((JmsDefaultPrefetchPolicy) factory.getPrefetchPolicy()).setAll(1);

        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);

        prefetchPolicy = (JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy();
        assertNotNull(prefetchPolicy);
        assertNotSame(factory.getPrefetchPolicy(), prefetchPolicy);

        assertEquals(1, prefetchPolicy.getTopicPrefetch());
        assertEquals(1, prefetchPolicy.getQueuePrefetch());
        assertEquals(1, prefetchPolicy.getQueueBrowserPrefetch());
        assertEquals(1, prefetchPolicy.getDurableTopicPrefetch());

        connection.close();
    }

    @Test
    public void testConnectionFactoryPresettlePolicyIsAppliedToConnection() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(USER, PASSWORD, "mock://localhost");

        JmsDefaultPresettlePolicy presettlePolicy = (JmsDefaultPresettlePolicy) factory.getPresettlePolicy();

        assertFalse(presettlePolicy.isPresettleAll());

        presettlePolicy.setPresettleAll(true);

        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);

        presettlePolicy = (JmsDefaultPresettlePolicy) connection.getPresettlePolicy();
        assertNotNull(presettlePolicy);
        assertNotSame(factory.getPresettlePolicy(), presettlePolicy);

        assertTrue(presettlePolicy.isPresettleAll());

        connection.close();
    }

    @Test
    public void testConnectionFactoryRedeliveryPolicyIsAppliedToConnection() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(USER, PASSWORD, "mock://localhost");

        JmsDefaultRedeliveryPolicy redeliveryPolicy = (JmsDefaultRedeliveryPolicy) factory.getRedeliveryPolicy();

        assertFalse(redeliveryPolicy.getMaxRedeliveries() == 100);

        redeliveryPolicy.setMaxRedeliveries(100);

        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);

        redeliveryPolicy = (JmsDefaultRedeliveryPolicy) connection.getRedeliveryPolicy();
        assertNotNull(redeliveryPolicy);
        assertNotSame(factory.getRedeliveryPolicy(), redeliveryPolicy);

        assertEquals(100, redeliveryPolicy.getMaxRedeliveries());

        connection.close();
    }

    @Test
    public void testConnectionFactoryDeserializationPolicyIsAppliedToConnection() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory(USER, PASSWORD, "mock://localhost");

        final String TRUSTED_PACKAGES = "java.lang,java.util";

        JmsDefaultDeserializationPolicy deserializationPolicy =
            (JmsDefaultDeserializationPolicy) factory.getDeserializationPolicy();

        assertFalse(deserializationPolicy.getAllowList().equals(TRUSTED_PACKAGES));

        deserializationPolicy.setWhiteList(TRUSTED_PACKAGES);

        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);

        deserializationPolicy = (JmsDefaultDeserializationPolicy) connection.getDeserializationPolicy();
        assertNotNull(deserializationPolicy);
        assertNotSame(factory.getDeserializationPolicy(), deserializationPolicy);

        assertEquals(TRUSTED_PACKAGES, deserializationPolicy.getAllowList());
    }

    @Test
    public void testConnectionGetConfiguredURIApplied() throws Exception {
        URI mock = new URI("mock://localhost");

        JmsConnectionFactory factory = new JmsConnectionFactory(mock);

        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertEquals(mock, connection.getConfiguredURI());
        connection.start();
        assertEquals(mock, connection.getConnectedURI());

        connection.close();
    }

    @Test
    public void testGlobalExceptionListenerIsAppliedToCreatedConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(new URI("mock://127.0.0.1:5763"));

        ExceptionListener listener = new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
            }
        };

        factory.setExceptionListener(listener);
        Connection connection = factory.createConnection();
        assertNotNull(connection);
        assertNotNull(connection.getExceptionListener());
        assertSame(listener, connection.getExceptionListener());

        connection.close();
    }

    @Test
    public void testUserConnectionIDGeneratorIsUedToCreatedConnection() throws Exception {
        IdGenerator userGenerator = new IdGenerator("TEST-ID:");

        JmsConnectionFactory factory = new JmsConnectionFactory(new URI("mock://127.0.0.1:5763"));
        factory.setConnectionIdGenerator(userGenerator);

        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        assertTrue(connection.getId().toString().startsWith("TEST-ID:"), "Connection ID = " + connection.getId());

        connection.close();
    }

    @Test
    public void testUserConnectionIDPrefixIsUedToCreatedConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(new URI("mock://127.0.0.1:5763"));
        factory.setConnectionIDPrefix("TEST-ID:");

        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        assertTrue(connection.getId().toString().startsWith("TEST-ID:"), "Connection ID = " + connection.getId());

        connection.close();
    }

    @Test
    public void testUserClientIDGeneratorIsUedToCreatedConnection() throws Exception {
        IdGenerator userGenerator = new IdGenerator("TEST-ID:");

        JmsConnectionFactory factory = new JmsConnectionFactory(new URI("mock://127.0.0.1:5763"));
        factory.setClientIdGenerator(userGenerator);

        JmsConnection connection = (JmsConnection) factory.createConnection();
        connection.start();

        assertNotNull(connection);
        assertTrue(connection.getClientID().startsWith("TEST-ID:"), "Connection ID = " + connection.getClientID());

        connection.close();
    }

    @Test
    public void testUserClientIDPrefixIsUedToCreatedConnection() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(new URI("mock://127.0.0.1:5763"));
        factory.setClientIDPrefix("TEST-ID:");

        JmsConnection connection = (JmsConnection) factory.createConnection();
        connection.start();

        assertNotNull(connection);
        assertTrue(connection.getClientID().startsWith("TEST-ID:"), "Client ID = " + connection.getClientID());

        connection.close();
    }

    @Test
    public void testCreateConnectionBadProviderURI() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(new URI("bad://127.0.0.1:5763"));

        try {
            factory.createConnection();
            fail("Should have thrown exception");
        } catch (JMSException jmse) {
            // expected
        }
    }

    @Test
    public void testCreateConnectionBadProviderString() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory("bad://127.0.0.1:5763");

        try {
            factory.createConnection();
            fail("Should have thrown exception");
        } catch (JMSException jmse) {
            // expected
        }
    }

    @Test
    public void testBadUriOptionCausesFail() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> {
            new JmsConnectionFactory("amqp://localhost:1234?jms.badOption=true");
        });
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
        assertNotEquals(clientID, cf.getClientID(), "value should not match yet");
        assertNotEquals(queuePrefix, cf.getQueuePrefix(), "value should not match yet");
        assertNotEquals(baseUri, cf.getRemoteURI(), "value should not match yet");

        // Set the properties
        Map<String, String> props = new HashMap<String, String>();
        // Add the URI property, itself containing a property option in its query
        props.put("remoteURI", uri);
        // Add another property directly
        props.put("queuePrefix", queuePrefix);
        Map<String, String> unusedProps = cf.setProperties(props);

        // Verify the clientID property option from the URI was applied.
        assertEquals(clientID, cf.getClientID(), "uri property query option not applied as expected");
        // Verify the direct property was applied
        assertEquals(queuePrefix, cf.getQueuePrefix(), "direct property not applied as expected");
        // Verify the URI was filtered to remove the applied options
        assertEquals(baseUri, cf.getRemoteURI(), "URI was filtered to remove options that were applied");

        // Verify the returned map was empty and unmodifiable
        assertTrue(unusedProps.isEmpty(), "Map should be empty: " + unusedProps);
        try {
            unusedProps.put("a", "b");
            fail("Map should be unmodifiable");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test
    public void testSetPropertiesWithUnusedOptions() throws Exception {
        String uri = "amqp://localhost:1234";
        String unusedKey = "unusedKey";
        String unusedValue = "unusedValue";

        // Create a connection factory object
        JmsConnectionFactory cf = new JmsConnectionFactory();

        // Verify the outcome conditions have not been met already
        assertNotEquals(uri, cf.getRemoteURI(), "value should not match yet");

        // Set the properties
        Map<String, String> props = new HashMap<String, String>();
        // Add a property that will get used
        props.put("remoteURI", uri);
        // Add a property that wont get used
        props.put(unusedKey, unusedValue);
        Map<String, String> unusedProps = cf.setProperties(props);

        // Verify the URI property was applied.
        assertEquals(uri, cf.getRemoteURI(), "uri property option not applied as expected");

        //Verify that the unused property was returned
        assertEquals(1, unusedProps.size(), "Unexpected size of return map");
        assertTrue(unusedProps.containsKey(unusedKey), "Expected property not found in map: " + unusedProps);
        assertEquals(unusedValue, unusedProps.get(unusedKey), "Unexpected property value");

        // Verify the returned map was unmodifiable
        try {
            unusedProps.put("a", "b");
            fail("Map should be unmodifiable");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test
    public void testSetPropertiesWithBadUriOptionCausesFail() throws Exception {
        JmsConnectionFactory cf = new JmsConnectionFactory();

        Map<String, String> props = new HashMap<String, String>();
        props.put("remoteURI", "amqp://localhost:1234?jms.badOption=true");

        try {
            cf.setProperties(props);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException iae) {
            // expected
        }
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
        assertTrue(props.containsKey(CLIENT_ID_PROP), CLIENT_ID_PROP + " property not found");
        assertEquals(clientID, props.get(CLIENT_ID_PROP), "clientID uri property query option not applied as expected");
        assertTrue(props.containsKey(QUEUE_PREFIX_PROP), QUEUE_PREFIX_PROP + " property not found");
        assertEquals(queuePrefix, props.get(QUEUE_PREFIX_PROP), "queue prefix property not applied as expected");
    }

    @Test
    public void testSerializeThenDeserialize() throws Exception {
        String uri = "amqp://localhost:1234";

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull(roundTripped, "Null object returned");
        assertEquals(JmsConnectionFactory.class, roundTripped.getClass(), "Unexpected type");
        assertEquals(uri, ((JmsConnectionFactory)roundTripped).getRemoteURI(), "Unexpected uri");

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertEquals(props, props2, "Properties were not equal");
    }

    /**
     * The prefetch policy is maintained in a child-object, which we extract the properties from
     * when serializing the factory. Ensure this functions by doing a round trip on a factory
     * configured with some new prefetch configuration via the URI.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSerializeThenDeserializeMaintainsPrefetchPolicy() throws Exception {
        String topicPrefetchValue = "17";
        String topicPrefetchKey = "prefetchPolicy.topicPrefetch";
        String uri = "amqp://localhost:1234?jms." + topicPrefetchKey + "=" + topicPrefetchValue;

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        assertTrue(props.containsKey(topicPrefetchKey), "Props dont contain expected prefetch policy change");
        assertEquals(topicPrefetchValue, props.get(topicPrefetchKey), "Unexpected value");

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull(roundTripped, "Null object returned");
        assertEquals(JmsConnectionFactory.class, roundTripped.getClass(), "Unexpected type");

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertTrue(props2.containsKey(topicPrefetchKey), "Props dont contain expected prefetch policy change");
        assertEquals(topicPrefetchValue, props2.get(topicPrefetchKey), "Unexpected value");

        assertEquals(props, props2, "Properties were not equal");
    }

    /**
     * The redelivery policy is maintained in a child-object, which we extract the properties from
     * when serializing the factory. Ensure this functions by doing a round trip on a factory
     * configured with some new redelivery configuration via the URI.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSerializeThenDeserializeMaintainsRedeliveryPolicy() throws Exception {
        String maxRedeliveryValue = "5";
        String maxRedeliveryKey = "redeliveryPolicy.maxRedeliveries";
        String uri = "amqp://localhost:1234?jms." + maxRedeliveryKey + "=" + maxRedeliveryValue;

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        assertTrue(props.containsKey(maxRedeliveryKey), "Props dont contain expected redelivery policy change");
        assertEquals(maxRedeliveryValue, props.get(maxRedeliveryKey), "Unexpected value");

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull(roundTripped, "Null object returned");
        assertEquals(JmsConnectionFactory.class, roundTripped.getClass(), "Unexpected type");

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertTrue(props2.containsKey(maxRedeliveryKey), "Props dont contain expected redelivery policy change");
        assertEquals(maxRedeliveryValue, props2.get(maxRedeliveryKey), "Unexpected value");

        assertEquals(props, props2, "Properties were not equal");
    }

    /**
     * The presettle policy is maintained in a child-object, which we extract the properties from
     * when serializing the factory. Ensure this functions by doing a round trip on a factory
     * configured with some new presettle configuration via the URI.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSerializeThenDeserializeMaintainsPresettlePolicy() throws Exception {
        String presettleAllValue = "true";
        String presettleAllKey = "presettlePolicy.presettleAll";
        String uri = "amqp://localhost:1234?jms." + presettleAllKey + "=" + presettleAllValue;

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        assertTrue(props.containsKey(presettleAllKey), "Props dont contain expected presettle policy change");
        assertEquals(presettleAllValue, props.get(presettleAllKey), "Unexpected value");

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull(roundTripped, "Null object returned");
        assertEquals(JmsConnectionFactory.class, roundTripped.getClass(), "Unexpected type");

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertTrue(props2.containsKey(presettleAllKey), "Props dont contain expected presettle policy change");
        assertEquals(presettleAllValue, props2.get(presettleAllKey), "Unexpected value");

        assertEquals(props, props2, "Properties were not equal");
    }

    /**
     * The message ID policy is maintained in a child-object, which we extract the properties from
     * when serializing the factory. Ensure this functions by doing a round trip on a factory
     * configured with some new message ID configuration via the URI.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSerializeThenDeserializeMaintainsMessageIDPolicy() throws Exception {
        String messageIDTypeValue = "UUID";
        String messageIDTypeKey = "messageIDPolicy.messageIDType";
        String uri = "amqp://localhost:1234?jms." + messageIDTypeKey + "=" + messageIDTypeValue;

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        assertTrue(props.containsKey(messageIDTypeKey), "Props dont contain expected message ID policy change");
        assertEquals(messageIDTypeValue, props.get(messageIDTypeKey), "Unexpected value");

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull(roundTripped, "Null object returned");
        assertEquals(JmsConnectionFactory.class, roundTripped.getClass(), "Unexpected type");

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertTrue(props2.containsKey(messageIDTypeKey), "Props dont contain expected message ID policy change");
        assertEquals(messageIDTypeValue, props2.get(messageIDTypeKey), "Unexpected value");

        assertEquals(props, props2, "Properties were not equal");
    }

    /**
     * The deserialization policy is maintained in a child-object, which we extract the properties from
     * when serializing the factory. Ensure this functions by doing a round trip on a factory
     * configured with some new deserialization configuration via the URI.
     *
     * @throws Exception if an error occurs during the test.
     *
     * @deprecated Remove this test when removing the deprecated configuration options
     */
    @Deprecated
    @Test
    public void testSerializeThenDeserializeMaintainsDeserializationPolicyDeprecated() throws Exception {
        String allowListValue = "java.lang";
        String allowListKey = "deserializationPolicy.whiteList";

        String denyListValue = "java.lang.foo";
        String denyListKey = "deserializationPolicy.blackList";

        String uri = "amqp://localhost:1234?jms." + allowListKey + "=" + allowListValue + "&jms." + denyListKey + "=" + denyListValue;

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        assertTrue(props.containsKey(allowListKey), "Props dont contain expected deserialization policy change");
        assertEquals(allowListValue, props.get(allowListKey), "Unexpected value");

        assertTrue(props.containsKey(denyListKey), "Props dont contain expected deserialization policy change");
        assertEquals(denyListValue, props.get(denyListKey), "Unexpected value");

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull(roundTripped, "Null object returned");
        assertEquals(JmsConnectionFactory.class, roundTripped.getClass(), "Unexpected type");

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertTrue(props2.containsKey(allowListKey), "Props dont contain expected deserialization policy change");
        assertEquals(allowListValue, props2.get(allowListKey), "Unexpected value");

        assertTrue(props2.containsKey(denyListKey), "Props dont contain expected deserialization policy change");
        assertEquals(denyListValue, props2.get(denyListKey), "Unexpected value");

        assertEquals(props, props2, "Properties were not equal");
    }

    /**
     * The deserialization policy is maintained in a child-object, which we extract the properties from
     * when serializing the factory. Ensure this functions by doing a round trip on a factory
     * configured with some new deserialization configuration via the URI.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSerializeThenDeserializeMaintainsDeserializationPolicy() throws Exception {
        String allowListValue = "java.lang";
        String allowListKey = "deserializationPolicy.allowList";

        String denyListValue = "java.lang.foo";
        String denyListKey = "deserializationPolicy.denyList";

        String uri = "amqp://localhost:1234?jms." + allowListKey + "=" + allowListValue + "&jms." + denyListKey + "=" + denyListValue;

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        Map<String, String> props = cf.getProperties();

        assertTrue(props.containsKey(allowListKey), "Props dont contain expected deserialization policy change");
        assertEquals(allowListValue, props.get(allowListKey), "Unexpected value");

        assertTrue(props.containsKey(denyListKey), "Props dont contain expected deserialization policy change");
        assertEquals(denyListValue, props.get(denyListKey), "Unexpected value");

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull(roundTripped, "Null object returned");
        assertEquals(JmsConnectionFactory.class, roundTripped.getClass(), "Unexpected type");

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();
        assertTrue(props2.containsKey(allowListKey), "Props dont contain expected deserialization policy change");
        assertEquals(allowListValue, props2.get(allowListKey), "Unexpected value");

        assertTrue(props2.containsKey(denyListKey), "Props dont contain expected deserialization policy change");
        assertEquals(denyListValue, props2.get(denyListKey), "Unexpected value");

        assertEquals(props, props2, "Properties were not equal");
    }

    @Test
    public void testSetRemoteURIThrowsOnNullURI() throws Exception {
        JmsConnectionFactory cf = new JmsConnectionFactory();
        try {
            cf.setRemoteURI(null);
            fail("Should not allow a null URI to be set.");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testSetRemoteURIThrowsOnEmptyStringURI() throws Exception {
        JmsConnectionFactory cf = new JmsConnectionFactory();
        try {
            cf.setRemoteURI("");
            fail("Should not allow a empty URI to be set.");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testCreateWithNullURIRemoteURIThrows() throws Exception {
        try {
            new JmsConnectionFactory((URI) null);
            fail("Should not allow a null URI to be set.");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testCreateWithNullURIStringRemoteURIThrows() throws Exception {
        try {
            new JmsConnectionFactory((String) null);
            fail("Should not allow a null URI to be set.");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testCreateWithEmptyURIStringRemoteURIThrows() throws Exception {
        try {
            new JmsConnectionFactory("");
            fail("Should not allow a empty URI string to be set.");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testCreateWithCredentialsWithNullURIRemoteURIThrows() throws Exception {
        try {
            new JmsConnectionFactory("user", "pass", (URI) null);
            fail("Should not allow a null URI to be set.");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testCreateWithCredentialsWithNullURIStringRemoteURIThrows() throws Exception {
        try {
            new JmsConnectionFactory("user", "pass", (String) null);
            fail("Should not allow a null URI to be set.");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testCreateWithCredentialsWithEmptyURIStringRemoteURIThrows() throws Exception {
        try {
            new JmsConnectionFactory("user", "pass", "");
            fail("Should not allow a empty URI string to be set.");
        } catch (IllegalArgumentException e) {
        }
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

    /**
     * Verify that the 'global' exception listener set on the connection factory
     * is ignored when the factory gets serialized.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testSerializeThenDeserializeIgnoresGlobalExceptionListener() throws Exception {
        String uri = "amqp://localhost:1234";

        JmsConnectionFactory cf = new JmsConnectionFactory(uri);
        cf.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                // Nothing
            }
        });

        Map<String, String> props = cf.getProperties();

        Object roundTripped = roundTripSerialize(cf);

        assertNotNull(roundTripped, "Null object returned");
        assertEquals(JmsConnectionFactory.class, roundTripped.getClass(), "Unexpected type");
        assertEquals(uri, ((JmsConnectionFactory)roundTripped).getRemoteURI(), "Unexpected uri");

        Map<String, String> props2 = ((JmsConnectionFactory)roundTripped).getProperties();

        assertFalse(props.containsKey("exceptionListener"), "Properties map should not contain ExceptionListener");
        assertEquals(props, props2, "Properties were not equal");
    }

    @Test
    @Timeout(5)
    public void testCreateConnectionWithPortOutOfRange() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:567564562");

        try {
            factory.createConnection();
            fail("Should have thrown exception");
        } catch (JMSException jmse) {
            LOG.debug("Caught Ex -> ", jmse);
        }

        factory = new JmsConnectionFactory("amqp://127.0.0.1:5675645622");

        try {
            factory.createConnection();
            fail("Should have thrown exception");
        } catch (JMSException jmse) {
            LOG.debug("Caught Ex -> ", jmse);
        }
    }

    @Test
    @Timeout(5)
    public void testURIOptionPopulateJMSXUserID() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(
            "amqp://127.0.0.1:5672?jms.populateJMSXUserID=true");

        assertTrue(factory.isPopulateJMSXUserID());

        factory = new JmsConnectionFactory(
            "amqp://127.0.0.1:5672?jms.populateJMSXUserID=false");

        assertFalse(factory.isPopulateJMSXUserID());
    }

    @Test
    public void testConnectionFactoryCreateWithMalformedURI() {
        try {
            new JmsConnectionFactory("amqp:\\\\localhost:5672");
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException ex) {}
    }

    @Test
    public void testDeserializationPolicyRestsToDefault() {
        JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:5672");

        assertNotNull(factory.getDeserializationPolicy());
        factory.setDeserializationPolicy(null);
        assertNotNull(factory.getDeserializationPolicy());
        assertTrue(factory.getDeserializationPolicy() instanceof JmsDefaultDeserializationPolicy);
    }

    @Test
    public void testCustomDeserializationPolicyIsAppliedToConnections() throws JMSException {
        JmsConnectionFactory factory = new JmsConnectionFactory("mock://127.0.0.1:5672");

        assertNotNull(factory.getDeserializationPolicy());
        factory.setDeserializationPolicy(new SerializationTestSupport.TestJmsDeserializationPolicy());
        assertNotNull(factory.getDeserializationPolicy());
        assertTrue(factory.getDeserializationPolicy() instanceof SerializationTestSupport.TestJmsDeserializationPolicy);

        JmsConnection connection = (JmsConnection) factory.createConnection();

        assertTrue(connection.getDeserializationPolicy() instanceof SerializationTestSupport.TestJmsDeserializationPolicy);

        connection.close();
    }

    @Test
    public void testCreateContext() {
        JmsConnectionFactory factory = new JmsConnectionFactory("mock://127.0.0.1:5672");

        JMSContext context = factory.createContext();
        assertNotNull(context);
        assertEquals(JMSContext.AUTO_ACKNOWLEDGE, context.getSessionMode());

        context.close();
    }

    @Test
    public void testCreateContextWithUserAndPassword() {
        JmsConnectionFactory factory = new JmsConnectionFactory("mock://127.0.0.1:5672");

        JMSContext context = factory.createContext(USER, PASSWORD);
        assertNotNull(context);
        assertEquals(JMSContext.AUTO_ACKNOWLEDGE, context.getSessionMode());

        context.close();
    }

    @Test
    public void testCreateContextWithUserAndPasswordAndSessionMode() {
        JmsConnectionFactory factory = new JmsConnectionFactory("mock://127.0.0.1:5672");

        JMSContext context = factory.createContext(USER, PASSWORD, JMSContext.CLIENT_ACKNOWLEDGE);
        assertNotNull(context);
        assertEquals(JMSContext.CLIENT_ACKNOWLEDGE, context.getSessionMode());

        context.close();
    }

    @Test
    public void testCreateContextWithSessionMode() {
        JmsConnectionFactory factory = new JmsConnectionFactory("mock://127.0.0.1:5672");

        JMSContext context = factory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
        assertNotNull(context);
        assertEquals(JMSContext.CLIENT_ACKNOWLEDGE, context.getSessionMode());

        context.close();
    }


    @Test
    public void testCreateContextWithInvalidSessionMode() {
        JmsConnectionFactory factory = new JmsConnectionFactory("mock://127.0.0.1:5672");

        try {
            factory.createContext(-1);
            fail("Should have thrown an JMSRuntimeException");
        } catch (JMSRuntimeException ex) {
        } catch (Throwable e) {
            fail("Wrong exception type thrown: " + e.getClass().getSimpleName());
        }

        try {
            factory.createContext("user", "pass", -1);
            fail("Should have thrown an JMSRuntimeException");
        } catch (JMSRuntimeException ex) {
        } catch (Throwable e) {
            fail("Wrong exception type thrown: " + e.getClass().getSimpleName());
        }
    }
}
