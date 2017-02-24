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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.jms.util.URISupport.CompositeData;
import org.junit.Test;

public class URISupportTest {

    //---- parseComposite ----------------------------------------------------//

    @Test
    public void testEmptyCompositePath() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("broker:()/localhost?persistent=false"));
        assertEquals(0, data.getComponents().size());
    }

    @Test
    public void testCompositePath() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("test:(path)/path"));
        assertEquals("path", data.getPath());
        data = URISupport.parseComposite(new URI("test:path"));
        assertNull(data.getPath());
    }

    @Test
    public void testSimpleComposite() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("test:part1"));
        assertEquals(1, data.getComponents().size());
    }

    @Test
    public void testParseComposite() throws Exception {
        URI uri = new URI("test:(part1://host,part2://(sub1://part,sube2:part))");
        CompositeData data = URISupport.parseComposite(uri);
        assertEquals(2, data.getComponents().size());

        assertFalse(URISupport.isCompositeURI(data.getComponents().get(0)));
        assertTrue(URISupport.isCompositeURI(data.getComponents().get(1)));

        assertTrue(data.getParameters().isEmpty());
    }

    @Test
    public void testParseCompositeWithInnerURIsWithQueryValues() throws Exception {
        URI uri = new URI("failover://(amqp://127.0.0.1:5678,amqp://127.0.0.2:5677,amqp://127.0.0.3:5676?amqp.traceFrames=false)?failover.useReconnectBackOff=false");
        CompositeData data = URISupport.parseComposite(uri);
        assertEquals(3, data.getComponents().size());

        Map<String, String> parameters = data.getParameters();
        assertEquals(1, parameters.size());

        assertTrue(parameters.containsKey("failover.useReconnectBackOff"));
        assertTrue(parameters.get("failover.useReconnectBackOff").equals("false"));

        List<URI> uris = data.getComponents();

        assertEquals(uris.get(0).toString(), "amqp://127.0.0.1:5678");
        assertEquals(uris.get(1).toString(), "amqp://127.0.0.2:5677");
        assertEquals(uris.get(2).toString(), "amqp://127.0.0.3:5676?amqp.traceFrames=false");
    }

    @Test
    public void testParseCompositeWithMismatchedParends() throws Exception {
        URI uri = new URI("test:(part1://host,part2://(sub1://part,sube2:part)");
        try {
            URISupport.parseComposite(uri);
            fail("Should not parse when parends don't match.");
        } catch (URISyntaxException ex) {
        }
    }

    @Test
    public void testEmptyCompositeWithParenthesisInParam() throws Exception {
        URI uri = new URI("failover://()?updateURIsURL=file:/C:/Dir(1)/a.csv");
        CompositeData data = URISupport.parseComposite(uri);
        assertEquals(0, data.getComponents().size());
        assertEquals(1, data.getParameters().size());
        assertTrue(data.getParameters().containsKey("updateURIsURL"));
        assertEquals("file:/C:/Dir(1)/a.csv", data.getParameters().get("updateURIsURL"));
    }

    @Test
    public void testCompositeWithParenthesisInParam() throws Exception {
        URI uri = new URI("failover://(test)?updateURIsURL=file:/C:/Dir(1)/a.csv");
        CompositeData data = URISupport.parseComposite(uri);
        assertEquals(1, data.getComponents().size());
        assertEquals(1, data.getParameters().size());
        assertTrue(data.getParameters().containsKey("updateURIsURL"));
        assertEquals("file:/C:/Dir(1)/a.csv", data.getParameters().get("updateURIsURL"));
    }

    @Test
    public void testCompositeWithComponentParam() throws Exception {
        CompositeData data = URISupport.parseComposite(new URI("test:(part1://host?part1=true)?outside=true"));
        assertEquals(1, data.getComponents().size());
        assertEquals(1, data.getParameters().size());
        Map<String, String> part1Params = URISupport.parseParameters(data.getComponents().get(0));
        assertEquals(1, part1Params.size());
        assertTrue(part1Params.containsKey("part1"));
    }

    @Test
    public void testParsingCompositeURI() throws URISyntaxException {
        CompositeData data = URISupport.parseComposite(new URI("broker://(tcp://localhost:61616)?name=foo"));
        assertEquals("one component", 1, data.getComponents().size());
        assertEquals("Size: " + data.getParameters(), 1, data.getParameters().size());
    }

    //---- parseQuery --------------------------------------------------------//

    @Test
    public void testParsingURI() throws Exception {
        URI source = new URI("tcp://localhost:61626/foo/bar?cheese=Edam&x=123");

        Map<String, String> map = PropertyUtil.parseQuery(source);

        assertEquals("Size: " + map, 2, map.size());
        assertMapKey(map, "cheese", "Edam");
        assertMapKey(map, "x", "123");

        URI result = URISupport.removeQuery(source);

        assertEquals("result", new URI("tcp://localhost:61626/foo/bar"), result);
    }

    @Test
    public void testParsingURIWithEmptyValuesInOptions() throws Exception {
        URI source = new URI("tcp://localhost:61626/foo/bar?cheese=&x=");

        Map<String, String> map = PropertyUtil.parseQuery(source);

        assertEquals("Size: " + map, 2, map.size());
        assertMapKey(map, "cheese", "");
        assertMapKey(map, "x", "");

        URI result = URISupport.removeQuery(source);

        assertEquals("result", new URI("tcp://localhost:61626/foo/bar"), result);
    }

    protected void assertMapKey(Map<String, String> map, String key, Object expected) {
        assertEquals("Map key: " + key, map.get(key), expected);
    }

    //---- checkParenthesis --------------------------------------------------------//

    @Test
    public void testCheckParenthesis() throws Exception {
        String str = "fred:(((ddd))";
        assertFalse(URISupport.checkParenthesis(str));
        str += ")";
        assertTrue(URISupport.checkParenthesis(str));
    }

    @Test
    public void testCheckParenthesisWithNullOrEmpty() throws Exception {
        assertTrue(URISupport.checkParenthesis(null));
        assertTrue(URISupport.checkParenthesis(""));
    }

    //---- replaceQuery ------------------------------------------------------//

    @Test
    public void testParsingParams() throws Exception {
        URI uri = new URI("static:(http://localhost:61617?proxyHost=jo&proxyPort=90)?proxyHost=localhost&proxyPort=80");
        Map<String,String>parameters = URISupport.parseParameters(uri);
        verifyParams(parameters);
        uri = new URI("static://http://localhost:61617?proxyHost=localhost&proxyPort=80");
        parameters = URISupport.parseParameters(uri);
        verifyParams(parameters);
        uri = new URI("http://0.0.0.0:61616");
        parameters = URISupport.parseParameters(uri);
        assertTrue(parameters.isEmpty());
        uri = new URI("failover:(http://0.0.0.0:61616)");
        parameters = URISupport.parseParameters(uri);
        assertTrue(parameters.isEmpty());
    }

    @Test
    public void testCreateWithQuery() throws Exception {
        URI source = new URI("vm://localhost");
        URI dest = PropertyUtil.replaceQuery(source, "network=true&one=two");

        assertEquals("correct param count", 2, URISupport.parseParameters(dest).size());
        assertEquals("same uri, host", source.getHost(), dest.getHost());
        assertEquals("same uri, scheme", source.getScheme(), dest.getScheme());
        assertFalse("same uri, ssp", dest.getQuery().equals(source.getQuery()));
    }

    @Test
    public void testApplyParameters() throws Exception {

        URI uri = new URI("http://0.0.0.0:61616");
        Map<String,String> parameters = new HashMap<String, String>();
        parameters.put("t.proxyHost", "localhost");
        parameters.put("t.proxyPort", "80");

        uri = URISupport.applyParameters(uri, parameters);
        Map<String,String> appliedParameters = URISupport.parseParameters(uri);
        assertEquals("all params applied  with no prefix", 2, appliedParameters.size());

        // strip off params again
        uri = PropertyUtil.eraseQuery(uri);

        uri = URISupport.applyParameters(uri, parameters, "joe");
        appliedParameters = URISupport.parseParameters(uri);
        assertTrue("no params applied as none match joe", appliedParameters.isEmpty());

        uri = URISupport.applyParameters(uri, parameters, "t.");
        verifyParams(URISupport.parseParameters(uri));
    }

    //---- parseParameters ---------------------------------------------------//

    @Test
    public void testCompositeCreateURIWithQuery() throws Exception {
        String queryString = "query=value";
        URI originalURI = new URI("outerscheme:(innerscheme:innerssp)");
        URI querylessURI = originalURI;
        assertEquals(querylessURI, PropertyUtil.eraseQuery(originalURI));
        assertEquals(querylessURI, PropertyUtil.replaceQuery(originalURI, ""));
        assertEquals(new URI(querylessURI + "?" + queryString), PropertyUtil.replaceQuery(originalURI, queryString));
        originalURI = new URI("outerscheme:(innerscheme:innerssp)?outerquery=0");
        assertEquals(querylessURI, PropertyUtil.eraseQuery(originalURI));
        assertEquals(querylessURI, PropertyUtil.replaceQuery(originalURI, ""));
        assertEquals(new URI(querylessURI + "?" + queryString), PropertyUtil.replaceQuery(originalURI, queryString));
        originalURI = new URI("outerscheme:(innerscheme:innerssp?innerquery=0)");
        querylessURI = originalURI;
        assertEquals(querylessURI, PropertyUtil.eraseQuery(originalURI));
        assertEquals(querylessURI, PropertyUtil.replaceQuery(originalURI, ""));
        assertEquals(new URI(querylessURI + "?" + queryString), PropertyUtil.replaceQuery(originalURI, queryString));
        originalURI = new URI("outerscheme:(innerscheme:innerssp?innerquery=0)?outerquery=0");
        assertEquals(querylessURI, PropertyUtil.eraseQuery(originalURI));
        assertEquals(querylessURI, PropertyUtil.replaceQuery(originalURI, ""));
        assertEquals(new URI(querylessURI + "?" + queryString), PropertyUtil.replaceQuery(originalURI, queryString));
    }

    @Test
    public void testApplyParametersPreservesOriginalParameters() throws Exception {
        URI uri = new URI("http://0.0.0.0:61616?timeout=1000");

        Map<String,String> parameters = new HashMap<String, String>();
        parameters.put("t.proxyHost", "localhost");
        parameters.put("t.proxyPort", "80");

        uri = URISupport.applyParameters(uri, parameters, "t.");
        Map<String,String> appliedParameters = URISupport.parseParameters(uri);
        assertEquals("all params applied with no prefix", 3, appliedParameters.size());
        verifyParams(appliedParameters);
    }

    @Test
    public void testApplyParametersOverwritesOriginalParameters() throws Exception {
        URI uri = new URI("http://0.0.0.0:61616?proxyHost=host&proxyPort=21&timeout=1000");

        Map<String,String> parameters = new HashMap<String, String>();
        parameters.put("proxyHost", "localhost");
        parameters.put("proxyPort", "80");

        uri = URISupport.applyParameters(uri, parameters);
        Map<String,String> appliedParameters = URISupport.parseParameters(uri);
        assertEquals("all params applied with no prefix", 3, appliedParameters.size());
        verifyParams(appliedParameters);
    }

    private void verifyParams(Map<String,String> parameters) {
        assertEquals("localhost", parameters.get("proxyHost"));
        assertEquals("80", parameters.get("proxyPort"));
    }

    //---- isCompositeURI ----------------------------------------------------//

    @Test
    public void testIsCompositeURIWithQueryNoSlashes() throws URISyntaxException {
        URI[] compositeURIs = new URI[] { new URI("test:(part1://host?part1=true)?outside=true"), new URI("broker:(tcp://localhost:61616)?name=foo") };
        for (URI uri : compositeURIs) {
            assertTrue(uri + " must be detected as composite URI", URISupport.isCompositeURI(uri));
        }
    }

    @Test
    public void testIsCompositeURIWithQueryAndSlashes() throws URISyntaxException {
        URI[] compositeURIs = new URI[] { new URI("test://(part1://host?part1=true)?outside=true"), new URI("broker://(tcp://localhost:61616)?name=foo") };
        for (URI uri : compositeURIs) {
            assertTrue(uri + " must be detected as composite URI", URISupport.isCompositeURI(uri));
        }
    }

    @Test
    public void testIsCompositeURINoQueryNoSlashes() throws URISyntaxException {
        URI[] compositeURIs = new URI[] { new URI("test:(part1://host,part2://(sub1://part,sube2:part))"), new URI("test:(path)/path") };
        for (URI uri : compositeURIs) {
            assertTrue(uri + " must be detected as composite URI", URISupport.isCompositeURI(uri));
        }
    }

    @Test
    public void testIsCompositeWhenURIHasUnmatchedParends() throws Exception {
        URI uri = new URI("test:(part1://host,part2://(sub1://part,sube2:part)");
        assertFalse(URISupport.isCompositeURI(uri));
    }

    @Test
    public void testIsCompositeURINoQueryNoSlashesNoParentheses() throws URISyntaxException {
        assertFalse("test:part1" + " must be detected as non-composite URI", URISupport.isCompositeURI(new URI("test:part1")));
    }

    @Test
    public void testIsCompositeURINoQueryWithSlashes() throws URISyntaxException {
        URI[] compositeURIs = new URI[] { new URI("failover://(tcp://bla:61616,tcp://bla:61617)"),
                new URI("failover://(tcp://localhost:61616,ssl://anotherhost:61617)") };
        for (URI uri : compositeURIs) {
            assertTrue(uri + " must be detected as composite URI", URISupport.isCompositeURI(uri));
        }
    }

    //---- indexOfParenthesisMatch -------------------------------------------//

    @Test
    public void testIndexOfParenthesisMatch() throws URISyntaxException {
        String source1 = "a(b)c";
        assertEquals(3, URISupport.indexOfParenthesisMatch(source1, 1));

        String source2 = "(b)";
        assertEquals(2, URISupport.indexOfParenthesisMatch(source2, 0));

        String source3 = "()";
        assertEquals(1, URISupport.indexOfParenthesisMatch(source3, 0));
    }

    @Test
    public void testIndexOfParenthesisMatchWhenNoMatchPresent() throws URISyntaxException {
        try {
            String source = "a(bc";
            URISupport.indexOfParenthesisMatch(source, 1);
            fail("Should have thrown URISyntaxException");
        } catch (URISyntaxException use) {}

        try {
            String source = "(";
            URISupport.indexOfParenthesisMatch(source, 0);
            fail("Should have thrown URISyntaxException");
        } catch (URISyntaxException use) {}

        try {
            String source = "a(";
            URISupport.indexOfParenthesisMatch(source, 1);
            fail("Should have thrown URISyntaxException");
        } catch (URISyntaxException use) {}
    }

    @Test
    public void testIndexOfParenthesisMatchExceptions() throws URISyntaxException {
        try {
            URISupport.indexOfParenthesisMatch(null, -1);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iobe) {}

        try {
            URISupport.indexOfParenthesisMatch("tcp", 4);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iobe) {}

        try {
            URISupport.indexOfParenthesisMatch("tcp", 2);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iobe) {}

        try {
            URISupport.indexOfParenthesisMatch("(tcp", 0);
            fail("Should have thrown URISyntaxException");
        } catch (URISyntaxException iobe) {}
    }

    //---- applyParameters ---------------------------------------------------//

    @Test
    public void testApplyParametersWithNullOrEmptyParameters() throws URISyntaxException {
        URI uri = new URI("tcp://localhost");

        URI result = URISupport.applyParameters(uri, null, "value.");
        assertSame(uri, result);

        result = URISupport.applyParameters(uri, Collections.<String, String>emptyMap(), "value.");
        assertSame(uri, result);
    }
}
