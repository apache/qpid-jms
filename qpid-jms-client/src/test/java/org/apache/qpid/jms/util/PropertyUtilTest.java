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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import org.junit.Test;

/**
 * Tests for URI and Class level property Utilities class.
 */
public class PropertyUtilTest {

    @SuppressWarnings("unused")
    private static final class Embedded {
        private String option;

        public void setOption(String option) {
            this.option = option;
        }

        public String getOption() {
            return option;
        }
    }

    @SuppressWarnings("unused")
    private static final class Options {
        private String firstOption;
        private String secondOption;
        private String throwsWhenSet;
        private String requiresTwo;
        private String notReadable;
        private int intOption;

        private int numberValue;
        private boolean booleanValue;
        private URL urlValue;
        private URI uriValue;
        private String[] stringArray;
        private SSLContext context;

        private Embedded embedded = new Embedded();

        public Options() {
        }

        public Options(String first, String last) {
            this.firstOption = first;
            this.secondOption = last;
        }

        public String getFirstName() {
            return this.firstOption;
        }

        public void setFirstName(String name) {
            this.firstOption = name;
        }

        public String getLastName() {
            return this.secondOption;
        }

        public void setLastName(String name) {
            this.secondOption = name;
        }

        public String getThrowsWhenSet() {
            return throwsWhenSet;
        }

        public void setThrowsWhenSet(String value) {
            throw new RuntimeException("Can't set this");
        }

        public String getRequiresTwo() {
            return requiresTwo;
        }

        public void setRequiresTwo(String value, boolean trigger) {
            if (trigger) this.requiresTwo = value;
        }

        public Number getNumberProperty() {
            return numberValue;
        }

        public Boolean getBooleanProperty() {
            return booleanValue;
        }

        public URI getURIValue() {
            return uriValue;
        }

        public void setURIValue(URI uriValue) {
            this.uriValue = uriValue;
        }

        public URL getURLValue() {
            return urlValue;
        }

        public void setURLValue(URL urlValue) {
            this.urlValue = urlValue;
        }

        public SSLContext getSSLContext() {
            return context;
        }

        public void setSSLContext(SSLContext context) {
            this.context = context;
        }

        public Embedded getEmbedded() {
            return embedded;
        }

        public void setEmbedded(Embedded embedded) {
            this.embedded = embedded;
        }

        public void setNotReadable(String value) {
            this.notReadable = value;
        }

        public String[] getStringArray() {
            return stringArray;
        }

        public void setStringArray(String[] stringArray) {
            this.stringArray = stringArray;
        }

        public void setIntOption(int value) {
            this.intOption = value;
        }

        public void setIntOption(String value) {
            this.intOption = Integer.parseInt(value);
        }

        public int getIntOption() {
            return intOption;
        }
    }

    @Test
    public void test() {
        new PropertyUtil();
    }

    //----- replaceQuery -----------------------------------------------------//

    @Test
    public void testReplaceQueryUsingMap() throws URISyntaxException {
        URI original = new URI("http://www.example.com?option=true");

        Map<String, String> newQuery = new HashMap<String, String>();
        newQuery.put("param", "replaced");

        URI updated = PropertyUtil.replaceQuery(original, newQuery);

        assertEquals("param=replaced", updated.getQuery());
    }

    @Test
    public void testReplaceQueryUsingString() throws URISyntaxException {
        URI original = new URI("http://www.example.com?option=true");
        URI updated = PropertyUtil.replaceQuery(original, "param=replaced");

        assertEquals("param=replaced", updated.getQuery());
    }

    @Test
    public void testReplaceQueryPreservesFragment() throws URISyntaxException {
        URI original = new URI("http://www.example.com?option=true#fragment");
        URI updated = PropertyUtil.replaceQuery(original, "param=replaced");

        assertEquals("param=replaced", updated.getQuery());
        assertEquals("fragment", updated.getFragment());
    }

    @Test
    public void testReplaceQueryPreservesFragmentWhenNoQueryPresent() throws URISyntaxException {
        URI original = new URI("http://www.example.com#fragment");
        URI updated = PropertyUtil.replaceQuery(original, "param=replaced");

        assertEquals("param=replaced", updated.getQuery());
        assertEquals("fragment", updated.getFragment());
    }

    @Test
    public void testReplaceQueryWithStringDoesNotReencode() throws URISyntaxException {
        URI original = new URI("http://www.example.com?option=X");

        final String encodedValue = "%25Ca%2BHn%2Fav";
        final String decodedValue = "%Ca+Hn/av";

        final String encodedKey = "user%2Bname";
        final String decodedKey = "user+name";

        String newQuery = encodedKey + "=" + encodedValue;

        URI updated = PropertyUtil.replaceQuery(original, newQuery);

        assertEquals(encodedKey + "=" + encodedValue, updated.getRawQuery());
        assertEquals(decodedKey + "=" + decodedValue, updated.getQuery());
    }

    @Test
    public void testReplaceQueryUsingMapEncodesParameters() throws URISyntaxException {
        URI original = new URI("http://www.example.com?option=X");

        final String encodedValue = "%25Ca%2BHn%2Fav";
        final String decodedValue = "%Ca+Hn/av";

        final String encodedKey = "user%2Bname";
        final String decodedKey = "user+name";

        Map<String, String> newQuery = new HashMap<String, String>();
        newQuery.put(decodedKey, decodedValue);

        URI updated = PropertyUtil.replaceQuery(original, newQuery);

        assertEquals(encodedKey + "=" + encodedValue, updated.getRawQuery());
        assertEquals(decodedKey + "=" + decodedValue, updated.getQuery());
    }

    @Test
    public void testReplaceQueryIgnoresQueryInCompositeURI() throws URISyntaxException {
        URI original = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)");
        URI expected = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)?failover.maxReconnectAttempts=1");
        URI updated = PropertyUtil.replaceQuery(original, "failover.maxReconnectAttempts=1");

        assertEquals(expected, updated);
    }

    @Test
    public void testReplaceQueryIgnoresQueryAndFragmentInCompositeURI() throws URISyntaxException {
        URI original = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true#ignored)");
        URI expected = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true#ignored)?failover.maxReconnectAttempts=1");
        URI updated = PropertyUtil.replaceQuery(original, "failover.maxReconnectAttempts=1");

        assertEquals(expected, updated);
    }

    @Test
    public void testReplaceQueryIgnoresQueryInCompositeURIPreservesFragment() throws URISyntaxException {
        URI original = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)#fragment");
        URI expected = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)?failover.maxReconnectAttempts=1#fragment");
        URI updated = PropertyUtil.replaceQuery(original, "failover.maxReconnectAttempts=1");

        assertEquals(expected, updated);
    }

    @Test
    public void testReplaceQueryIgnoresQueryInCompositeURIReplaceExisting() throws URISyntaxException {
        URI original = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)?failover.maxReconnectAttempts=2");
        URI expected = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)?failover.maxReconnectAttempts=1");
        URI updated = PropertyUtil.replaceQuery(original, "failover.maxReconnectAttempts=1");

        assertEquals(expected, updated);
    }

    @Test
    public void testReplaceQueryIgnoresQueryInCompositeURIReplaceExistingPreservesFragment() throws URISyntaxException {
        URI original = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)?failover.maxReconnectAttempts=2#fragment");
        URI expected = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)?failover.maxReconnectAttempts=1#fragment");
        URI updated = PropertyUtil.replaceQuery(original, "failover.maxReconnectAttempts=1");

        assertEquals(expected, updated);
    }

    //----- eraseQuery -------------------------------------------------------//

    @Test
    public void testEraseQuery() throws URISyntaxException {
        URI original = new URI("http://www.example.com?option=true");
        URI updated = PropertyUtil.eraseQuery(original);

        assertNull(updated.getQuery());
    }

    @Test
    public void testEraseQueryPreservesFragment() throws URISyntaxException {
        URI original = new URI("http://www.example.com?option=true#fragment");
        URI updated = PropertyUtil.eraseQuery(original);

        assertNull(updated.getQuery());
        assertEquals("fragment", updated.getFragment());
    }

    @Test
    public void testEraseQueryPreservesFragmentWhenNoQueryPresent() throws URISyntaxException {
        URI original = new URI("http://www.example.com#fragment");
        URI updated = PropertyUtil.eraseQuery(original);

        assertNull(updated.getQuery());
        assertEquals("fragment", updated.getFragment());
    }

    @Test
    public void testEraseQueryIgnoresQueryInCompositeURI() throws URISyntaxException {
        URI original = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)");
        URI expected = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true)");
        URI updated = PropertyUtil.eraseQuery(original);

        assertEquals(expected, updated);
    }

    @Test
    public void testEraseQueryIgnoresQueryAndFragmentInCompositeURI() throws URISyntaxException {
        URI original = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true#ignored)?failover.maxReconnectAttempts=1");
        URI expected = new URI("failover:(amqp://example.com:5672?amqp.traceFrames=true#ignored)");
        URI updated = PropertyUtil.eraseQuery(original);

        assertEquals(expected, updated);
    }

    //----- createQuery ------------------------------------------------------//

    @Test
    public void testCreateQuery() throws URISyntaxException {
        Map<String, String> newQuery = new HashMap<String, String>();
        newQuery.put("param1", "value");
        newQuery.put("param2", "value");

        String query = PropertyUtil.createQueryString(newQuery);

        assertNotNull(query);
        assertEquals("param1=value&param2=value", query);
    }

    @Test
    public void testCreateQueryEncodesParameters() throws URISyntaxException {

        final String encodedValue = "%25Ca%2BHn%2Fav";
        final String decodedValue = "%Ca+Hn/av";

        final String encodedKey = "user%2Bname";
        final String decodedKey = "user+name";

        Map<String, String> source = new HashMap<String, String>();
        source.put(decodedKey, decodedValue);

        String result = PropertyUtil.createQueryString(source);

        assertEquals(encodedKey + "=" + encodedValue, result);
    }

    //----- parseQuery -------------------------------------------------------//

    @Test
    public void testParseQueryFromURI() throws Exception {
        URI original = new URI("http://www.example.com?option=true&another=false");
        Map<String, String> result = PropertyUtil.parseQuery(original);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("option"));
        assertTrue(result.containsKey("another"));
        assertEquals("true", result.get("option"));
        assertEquals("false", result.get("another"));
    }

    @Test
    public void testParseQueryFromURIWithNoQuery() throws Exception {
        URI original = new URI("http://www.example.com");
        Map<String, String> result = PropertyUtil.parseQuery(original);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseQueryFromNullURI() throws Exception {
        Map<String, String> result = PropertyUtil.parseQuery((URI) null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseQueryFromString() throws Exception {
        Map<String, String> result = PropertyUtil.parseQuery("option=true&another=false");

        assertTrue(result.size() == 2);
        assertTrue(result.containsKey("option"));
        assertTrue(result.containsKey("another"));
        assertEquals("true", result.get("option"));
        assertEquals("false", result.get("another"));
    }

    @Test
    public void testParseQueryFromStringWithNoValues() throws Exception {
        Map<String, String> result = PropertyUtil.parseQuery("option=&another=");

        assertTrue(result.size() == 2);
        assertTrue(result.containsKey("option"));
        assertTrue(result.containsKey("another"));
        assertEquals("", result.get("option"));
        assertEquals("", result.get("another"));
    }

    @Test
    public void testParseQueryFromNullURIString() throws Exception {
        Map<String, String> result = PropertyUtil.parseQuery((String) null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseQueryWithSingletonProperty() throws Exception {
        Map<String, String> result = PropertyUtil.parseQuery("option=true&another=false&notAssigned");

        assertTrue(result.size() == 3);
        assertTrue(result.containsKey("option"));
        assertTrue(result.containsKey("another"));
        assertTrue(result.containsKey("notAssigned"));
        assertEquals("true", result.get("option"));
        assertEquals("false", result.get("another"));
        assertEquals(null, result.get("notAssigned"));
    }

    @Test
    public void testParseQueryEmptryString() throws Exception {
        Map<String, String> result = PropertyUtil.parseQuery("");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseQueryNullString() throws Exception {
        Map<String, String> result = PropertyUtil.parseQuery((String) null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseQueryDecodesParameters() throws Exception {
        URI original = new URI("http://www.example.com?user%2Bname=%25Ca%2BHn%2Fav");

        final String decodedKey = "user+name";
        final String decodedValue = "%Ca+Hn/av";

        Map<String, String> result = PropertyUtil.parseQuery(original);

        assertEquals(1, result.size());
        assertTrue(result.containsKey(decodedKey));
        assertEquals(decodedValue, result.get(decodedKey));
    }

    //----- filterProperties -------------------------------------------------//

    @Test(expected=IllegalArgumentException.class)
    public void testFilterPropertiesNullProperties() throws Exception {
        PropertyUtil.filterProperties(null, "option.");
    }

    @Test
    public void testFilterPropertiesNoFilter() throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("unprefixied", "true");
        properties.put("option.filtered1", "true");
        properties.put("option.filtered2", "false");

        Map<String, String> result = PropertyUtil.filterProperties(properties, "option.");

        assertEquals(2, result.size());
        assertTrue(result.containsKey("filtered1"));
        assertTrue(result.containsKey("filtered2"));
        assertFalse(result.containsKey("unprefixed"));

        assertEquals("true", result.get("filtered1"));
        assertEquals("false", result.get("filtered2"));
    }

    //----- setProperties ----------------------------------------------------//

    @Test
    public void testSetProperties() throws Exception {
        Options configObject = new Options();

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("firstName", "foo");
        properties.put("lastName", "bar");

        assertTrue(PropertyUtil.setProperties(configObject, properties).isEmpty());

        assertEquals("foo", configObject.getFirstName());
        assertEquals("bar", configObject.getLastName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPropertiesThrowsOnNullObject() throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("firstName", "foo");
        properties.put("lastName", "bar");

        PropertyUtil.setProperties(null, properties);
    }

    @Test
    public void testSetPropertiesUsingPropertiesObject() throws Exception {
        Options configObject = new Options();

        Properties properties = new Properties();
        properties.put("firstName", "foo");
        properties.put("lastName", "bar");

        assertTrue(PropertyUtil.setProperties(configObject, properties).isEmpty());

        assertEquals("foo", configObject.getFirstName());
        assertEquals("bar", configObject.getLastName());
    }

    @Test
    public void testSetPropertiesWithUnusedOptions() throws Exception {
        Options configObject = new Options();

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("firstName", "foo");
        properties.put("lastName", "bar");
        properties.put("unused", "absent");

        Map<String, String> result = PropertyUtil.setProperties(configObject, properties);

        assertFalse(result.isEmpty());
        assertTrue(result.containsKey("unused"));

        assertEquals("foo", configObject.getFirstName());
        assertEquals("bar", configObject.getLastName());
    }

    @Test
    public void testSetPropertiesWithUnusedOptionsUsingPropertiesObject() throws Exception {
        Options configObject = new Options();

        Properties properties = new Properties();
        properties.put("firstName", "foo");
        properties.put("lastName", "bar");
        properties.put("unused", "absent");

        Map<String, Object> result = PropertyUtil.setProperties(configObject, properties);

        assertFalse(result.isEmpty());
        assertTrue(result.containsKey("unused"));

        assertEquals("foo", configObject.getFirstName());
        assertEquals("bar", configObject.getLastName());
    }

    //----- getProperties ----------------------------------------------------//

    @Test
    public void testGetProperties() throws Exception {
        Options configObject = new Options("foo", "bar");

        Map<String, String> properties = PropertyUtil.getProperties(configObject);

        assertFalse(properties.isEmpty());
        assertEquals("foo", properties.get("firstName"));
        assertEquals("bar", properties.get("lastName"));
    }

    @Test
    public void testGetPropertiesHandlesURIs() throws Exception {
        Options configObject = new Options("foo", "bar");

        configObject.setURIValue(new URI("test://test"));

        Map<String, String> properties = PropertyUtil.getProperties(configObject);

        assertFalse(properties.isEmpty());
        assertEquals("foo", properties.get("firstName"));
        assertEquals("bar", properties.get("lastName"));
        assertEquals("test://test", properties.get("URIValue"));
    }

    @Test
    public void testGetPropertiesHandlesURLs() throws Exception {
        Options configObject = new Options("foo", "bar");

        configObject.setURLValue(new URL("http://www.domain.com"));

        Map<String, String> properties = PropertyUtil.getProperties(configObject);

        assertFalse(properties.isEmpty());
        assertEquals("foo", properties.get("firstName"));
        assertEquals("bar", properties.get("lastName"));
        assertEquals("http://www.domain.com", properties.get("URLValue"));
    }

    @Test
    public void testGetPropertiesIgnoresSSLContext() throws Exception {
        Options configObject = new Options("foo", "bar");

        configObject.setSSLContext(SSLContext.getDefault());
        Map<String, String> properties = PropertyUtil.getProperties(configObject);

        assertFalse(properties.isEmpty());
        assertEquals("foo", properties.get("firstName"));
        assertEquals("bar", properties.get("lastName"));

        assertFalse(properties.containsKey("sslContext"));
    }

    @Test
    public void testGetProperty() throws Exception {
        Options configObject = new Options("foo", "bar");
        Object result = PropertyUtil.getProperty(configObject, "firstName");
        assertNotNull(result);
        assertTrue(result instanceof String);
        assertEquals("foo", result);
    }

    @Test
    public void testGetPropertyInvliadFieldName() throws Exception {
        Options configObject = new Options("foo", "bar");
        Object result = PropertyUtil.getProperty(configObject, "first");
        assertNull(result);
    }

    @Test
    public void testGetPropertyWithNoReadMethod() throws Exception {
        Options configObject = new Options("foo", "bar");
        Object result = PropertyUtil.getProperty(configObject, "notReadable");
        assertNull(result);
    }

    //----- setProperty ------------------------------------------------------//

    @Test
    public void testSetProperty() throws Exception {
        Options configObject = new Options();
        assertTrue(PropertyUtil.setProperty(configObject, "firstName", "foo"));
        assertEquals("foo", configObject.getFirstName());
    }

    @Test
    public void testSetPropertyOfURI() throws Exception {
        Options configObject = new Options();
        assertTrue(PropertyUtil.setProperty(configObject, "URIValue", "test://foo"));
        assertEquals("test://foo", configObject.getURIValue().toString());
    }

    @Test
    public void testSetPropertyToNull() throws Exception {
        Options configObject = new Options();
        configObject.setFirstName("foo");
        assertTrue(PropertyUtil.setProperty(configObject, "firstName", null));
        assertNull(configObject.getFirstName());
    }

    @Test
    public void testSetPropertyOnlyWorksOnSingleArgProperties() throws Exception {
        Options configObject = new Options();
        configObject.setRequiresTwo("unchanged", true);
        assertFalse(PropertyUtil.setProperty(configObject, "requiresTwo", "foo"));
        assertEquals("unchanged", configObject.getRequiresTwo());
    }

    @Test
    public void testSetPropertyWithWrongPropertyName() throws Exception {
        Options configObject = new Options("goo", "bar");
        assertFalse(PropertyUtil.setProperty(configObject, "first", "foo"));
        assertEquals("goo", configObject.getFirstName());
    }

    @Test
    public void testSetPropertyWithThrowOnSet() throws Exception {
        Options configObject = new Options("goo", "bar");
        assertFalse(PropertyUtil.setProperty(configObject, "throwsWhenSet", "foo"));
    }

    @Test
    public void testSetPropertyOfStringArray() throws Exception {
        Options configObject = new Options();
        assertTrue(PropertyUtil.setProperty(configObject, "stringArray", "foo,bar"));
        assertNotNull(configObject.getStringArray());
        assertEquals(2, configObject.getStringArray().length);
        assertEquals("foo", configObject.getStringArray()[0]);
        assertEquals("bar", configObject.getStringArray()[1]);
    }

    @Test
    public void testSetPropertyOfStringArrayWithNull() throws Exception {
        Options configObject = new Options();
        assertTrue(PropertyUtil.setProperty(configObject, "stringArray", null));
        assertNull(configObject.getStringArray());
    }

    @Test
    public void testSetPropertyOfStringArraySingleValue() throws Exception {
        Options configObject = new Options();
        assertTrue(PropertyUtil.setProperty(configObject, "stringArray", "foo"));
        assertNotNull(configObject.getStringArray() != null);
        assertEquals(1, configObject.getStringArray().length);
        assertEquals("foo", configObject.getStringArray()[0]);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetPropertiesWithNullObject() {
        PropertyUtil.setProperties(null, new HashMap<String, String>());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetPropertiesWithNullMap() {
        PropertyUtil.setProperties(new Options(), (Map<String, String>) null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetPropertiesWithNullProperties() {
        PropertyUtil.setProperties(new Options(), (Properties) null);
    }

    @Test
    public void testSetPropertyWithTwoSettersUsingString() throws Exception {
        Options configObject = new Options();
        assertTrue(PropertyUtil.setProperty(configObject, "intOption", "1"));
        assertEquals(1, configObject.getIntOption());
    }

    @Test
    public void testSetPropertyWithTwoSettersUsingInt() throws Exception {
        Options configObject = new Options();
        assertTrue(PropertyUtil.setProperty(configObject, "intOption", 1));
        assertEquals(1, configObject.getIntOption());
    }

    @Test
    public void testSetPropertyWithTwoSettersUsingBadValueIsNotApplied() throws Exception {
        Options configObject = new Options();
        assertFalse(PropertyUtil.setProperty(configObject, "intOption", Long.MAX_VALUE));
        assertEquals(0, configObject.getIntOption());
    }

    //----- stripPrefix ------------------------------------------------------//

    @Test
    public void testStripPrefix() {
        String value = "prefixed.option";
        String result = PropertyUtil.stripPrefix(value, "prefixed.");
        assertEquals("option", result);
    }

    @Test
    public void testStripPrefixNullPrefix() {
        String value = "prefixed.option";
        String result = PropertyUtil.stripPrefix(value, null);
        assertSame(value, result);
    }

    @Test
    public void testStripPrefixNullValue() {
        assertNull(PropertyUtil.stripPrefix((String) null, "prefixed."));
    }

    //----- stripBefore ------------------------------------------------------//

    @Test
    public void testStripBefore() {
        String value = "prefixed.option";
        String result = PropertyUtil.stripBefore(value, '.');
        assertEquals("prefixed", result);
    }

    @Test
    public void testStripBeforeNullString() {
        assertNull(PropertyUtil.stripBefore((String) null, '.'));
    }

    //----- stripUpto ------------------------------------------------------//

    @Test
    public void testStripUpTo() {
        String value = "prefixed.option";
        String result = PropertyUtil.stripUpto(value, '.');
        assertEquals("option", result);
    }

    @Test
    public void testStripUpToNullString() {
        assertNull(PropertyUtil.stripUpto((String) null, '.'));
    }

}
