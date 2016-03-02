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
    }

    @Test
    public void test() {
        new PropertyUtil();
    }

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
    public void testEraseQuery() throws URISyntaxException {
        URI original = new URI("http://www.example.com?option=true");
        URI updated = PropertyUtil.eraseQuery(original);

        assertNull(updated.getQuery());
    }

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
    public void testParseParametersFromURI() throws Exception {
        URI original = new URI("http://www.example.com?option=true&another=false");
        Map<String, String> result = PropertyUtil.parseParameters(original);

        assertEquals(2, result.size());
        assertTrue(result.containsKey("option"));
        assertTrue(result.containsKey("another"));
        assertEquals("true", result.get("option"));
        assertEquals("false", result.get("another"));
    }

    @Test
    public void testParseParametersFromURIWithNoQuery() throws Exception {
        URI original = new URI("http://www.example.com");
        Map<String, String> result = PropertyUtil.parseParameters(original);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseParametersFromNullURI() throws Exception {
        Map<String, String> result = PropertyUtil.parseParameters((URI) null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseParametersFromString() throws Exception {
        Map<String, String> result = PropertyUtil.parseParameters("http://www.example.com?option=true&another=false");

        assertTrue(result.size() == 2);
        assertTrue(result.containsKey("option"));
        assertTrue(result.containsKey("another"));
        assertEquals("true", result.get("option"));
        assertEquals("false", result.get("another"));
    }

    @Test
    public void testParseParametersFromURIStringWithNoQuery() throws Exception {
        Map<String, String> result = PropertyUtil.parseParameters("http://www.example.com");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseParametersFromNullURIString() throws Exception {
        Map<String, String> result = PropertyUtil.parseParameters((String) null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParseQuery() throws Exception {
        Map<String, String> result = PropertyUtil.parseQuery("option=true&another=false");

        assertTrue(result.size() == 2);
        assertTrue(result.containsKey("option"));
        assertTrue(result.containsKey("another"));
        assertEquals("true", result.get("option"));
        assertEquals("false", result.get("another"));
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

    @Test
    public void testAddPropertiesToURIFromBean() throws Exception {
        String uriBase = "www.example.com";
        Options configObject = new Options("foo", "bar");

        String result = PropertyUtil.addPropertiesToURIFromBean(uriBase, configObject);
        assertNotNull(result);
        URI resultURI = new URI(result);
        assertNotNull(resultURI.getQuery());

        Map<String, String> props = PropertyUtil.parseQuery(resultURI.getQuery());
        assertFalse(props.isEmpty());
        assertTrue(props.containsKey("firstName"));
        assertTrue(props.containsKey("lastName"));
    }

    @Test
    public void testAddPropertiesToURIFromNullBean() throws Exception {
        String uriBase = "www.example.com";
        String result = PropertyUtil.addPropertiesToURIFromBean(uriBase, (Options) null);
        assertNotNull(result);
        URI resultURI = new URI(result);
        assertNull(resultURI.getQuery());
    }

    @Test
    public void testAddPropertiesToStringURIFromMap() throws Exception {
        String uriBase = "www.example.com";

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("option1", "true");
        properties.put("option2", "false");

        String result = PropertyUtil.addPropertiesToURI(uriBase, properties);
        assertNotNull(result);
        URI resultURI = new URI(result);
        assertNotNull(resultURI.getQuery());

        Map<String, String> parsed = PropertyUtil.parseQuery(resultURI.getQuery());
        assertEquals(properties, parsed);
    }

    @Test
    public void testAddPropertiesToStringURIFromEmptyMap() throws Exception {
        String uriBase = "www.example.com";

        String result = PropertyUtil.addPropertiesToURI(uriBase, null);
        assertSame(uriBase, result);
    }

    @Test
    public void testAddPropertiesToStringURIWithNullURI() throws Exception {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("option1", "true");
        properties.put("option2", "false");

        String result = PropertyUtil.addPropertiesToURI((String) null, properties);
        assertNull(result);
    }

    @Test
    public void testAddPropertiesToStringURIFromMapKeepsExisting() throws Exception {
        String uriBase = "www.example.com?existing=keepMe";

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("option1", "true");
        properties.put("option2", "false");

        String result = PropertyUtil.addPropertiesToURI(uriBase, properties);
        assertNotNull(result);
        URI resultURI = new URI(result);
        assertNotNull(resultURI.getQuery());

        Map<String, String> parsed = PropertyUtil.parseQuery(resultURI.getQuery());
        assertEquals(3, parsed.size());
        assertTrue(parsed.containsKey("existing"));
        assertEquals("keepMe", parsed.get("existing"));
    }

    @Test
    public void testAddPropertiesToURIFromMap() throws Exception {
        URI uri = new URI("www.example.com");

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("option1", "true");
        properties.put("option2", "false");

        String result = PropertyUtil.addPropertiesToURI(uri, properties);
        assertNotNull(result);
        URI resultURI = new URI(result);
        assertNotNull(resultURI.getQuery());

        Map<String, String> parsed = PropertyUtil.parseQuery(resultURI.getQuery());
        assertEquals(properties, parsed);
    }

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
