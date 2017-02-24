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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides support methods for dealing with URI values.
 */
public class URISupport {

    /**
     * A composite URI can be split into one or more CompositeData object which each represent
     * the individual URIs that comprise the composite one.
     */
    public static class CompositeData {

        private String host;
        private String scheme;
        private String path;
        private List<URI> components = Collections.emptyList();
        private Map<String, String> parameters = Collections.emptyMap();
        private String fragment;

        public List<URI> getComponents() {
            return components;
        }

        public String getFragment() {
            return fragment;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public String getScheme() {
            return scheme;
        }

        public String getPath() {
            return path;
        }

        public String getHost() {
            return host;
        }

        public URI toURI() throws URISyntaxException {
            StringBuffer sb = new StringBuffer();
            if (scheme != null) {
                sb.append(scheme);
                sb.append(':');
            }

            if (host != null && host.length() != 0) {
                sb.append(host);
            } else {
                sb.append('(');
                for (int i = 0; i < components.size(); i++) {
                    if (i != 0) {
                        sb.append(',');
                    }
                    sb.append(components.get(i).toString());
                }
                sb.append(')');
            }

            if (path != null) {
                sb.append('/');
                sb.append(path);
            }
            if (!parameters.isEmpty()) {
                sb.append("?");
                sb.append(PropertyUtil.createQueryString(parameters));
            }
            if (fragment != null) {
                sb.append("#");
                sb.append(fragment);
            }
            return new URI(sb.toString());
        }
    }

    /**
     * Given a composite URI, parse the individual URI elements contained within that URI and
     * return a CompsoteData instance that contains the parsed URI values.
     *
     * @param uri
     *        The target URI that should be parsed.
     *
     * @return a new CompsiteData instance representing the parsed composite URI.
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static CompositeData parseComposite(URI uri) throws URISyntaxException {

        CompositeData rc = new CompositeData();
        rc.scheme = uri.getScheme();
        String ssp = PropertyUtil.stripPrefix(uri.getRawSchemeSpecificPart().trim(), "//").trim();

        try {
            parseComposite(uri, rc, ssp);
        } catch (Exception e) {
            throw new URISyntaxException(uri.toString(), e.getMessage());
        }

        rc.fragment = uri.getFragment();
        return rc;
    }

    /**
     * Given a composite URI and a CompositeData instance and the scheme specific part extracted
     * from the source URI, parse the composite URI and populate the CompositeData object with
     * the results. The source URI is used only for logging as the ssp should have already been
     * extracted from it and passed here.
     *
     * @param uri
     *        The original source URI whose ssp is parsed into the composite data.
     * @param rc
     *        The CompsositeData instance that will be populated from the given ssp.
     * @param ssp
     *        The scheme specific part from the original string that is a composite or one or
     *        more URIs.
     *
     * @throws URISyntaxException
     */
    private static void parseComposite(URI uri, CompositeData rc, String ssp) throws Exception {
        String componentString;
        String params;

        if (!checkParenthesis(ssp)) {
            throw new URISyntaxException(uri.toString(), "Not a matching number of '(' and ')' parenthesis");
        }

        int p;
        int initialParen = ssp.indexOf("(");
        if (initialParen == 0) {

            rc.host = ssp.substring(0, initialParen);
            p = rc.host.indexOf("/");

            if (p >= 0) {
                rc.path = rc.host.substring(p);
                rc.host = rc.host.substring(0, p);
            }

            p = indexOfParenthesisMatch(ssp, initialParen);
            componentString = ssp.substring(initialParen + 1, p);
            params = ssp.substring(p + 1).trim();

        } else {
            componentString = ssp;
            params = "";
        }

        String components[] = splitComponents(componentString);
        rc.components = new ArrayList<URI>(components.length);
        for (int i = 0; i < components.length; i++) {
            rc.components.add(new URI(components[i].trim()));
        }

        p = params.indexOf("?");
        if (p >= 0) {
            if (p > 0) {
                rc.path = PropertyUtil.stripPrefix(params.substring(0, p), "/");
            }
            rc.parameters = PropertyUtil.parseQuery(params.substring(p + 1));
        } else {
            if (params.length() > 0) {
                rc.path = PropertyUtil.stripPrefix(params, "/");
            }
            rc.parameters = Collections.emptyMap();
        }
    }

    /**
     * Examine a URI and determine if it is a Composite type or not.
     *
     * @param uri
     *        The URI that is to be examined.
     *
     * @return true if the given URI is a Composite type.
     */
    public static boolean isCompositeURI(URI uri) {
        String ssp = PropertyUtil.stripPrefix(uri.getRawSchemeSpecificPart().trim(), "//").trim();

        if (ssp.indexOf('(') == 0 && checkParenthesis(ssp)) {
            return true;
        }

        return false;
    }

    /**
     * Examine the supplied string and ensure that all parends appear as matching pairs.
     *
     * @param str
     *        The target string to examine.
     *
     * @return true if the target string has valid parend pairings.
     */
    public static boolean checkParenthesis(String str) {
        boolean result = true;
        if (str != null) {
            int open = 0;
            int closed = 0;

            int i = 0;
            while ((i = str.indexOf('(', i)) >= 0) {
                i++;
                open++;
            }
            i = 0;
            while ((i = str.indexOf(')', i)) >= 0) {
                i++;
                closed++;
            }
            result = open == closed;
        }

        return result;
    }

    /**
     * Given a string and a position in that string of an open parend, find the matching close
     * parend.
     *
     * @param str
     *        The string to be searched for a matching parend.
     * @param first
     *        The index in the string of the opening parend whose close value is to be searched.
     *
     * @return the index in the string where the closing parend is located.
     *
     * @throws URISyntaxException
     *         if the string does not contain a matching parend.
     */
    public static int indexOfParenthesisMatch(String str, int first) throws URISyntaxException {
        int index = -1;

        if (first < 0 || first > str.length()) {
            throw new IllegalArgumentException("Invalid position for first parenthesis: " + first);
        }

        if (str.charAt(first) != '(') {
            throw new IllegalArgumentException("character at indicated position is not a parenthesis");
        }

        int depth = 1;
        char[] array = str.toCharArray();
        for (index = first + 1; index < array.length; ++index) {
            char current = array[index];
            if (current == '(') {
                depth++;
            } else if (current == ')') {
                if (--depth == 0) {
                    break;
                }
            }
        }

        if (depth != 0) {
            throw new URISyntaxException(str, "URI did not contain a matching parenthesis.");
        }

        return index;
    }

    /**
     * Given the inner portion of a composite URI, split and return each inner URI as a string
     * element in a new String array.
     *
     * @param str
     *        The inner URI elements of a composite URI string.
     *
     * @return an array containing each inner URI from the composite one.
     */
    public static String[] splitComponents(String str) {
        List<String> l = new ArrayList<String>();

        int last = 0;
        int depth = 0;
        char chars[] = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            switch (chars[i]) {
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    break;
                case ',':
                    if (depth == 0) {
                        String s = str.substring(last, i);
                        l.add(s);
                        last = i + 1;
                    }
                    break;
                default:
            }
        }

        String s = str.substring(last);
        if (s.length() != 0) {
            l.add(s);
        }

        String rc[] = new String[l.size()];
        l.toArray(rc);
        return rc;
    }

    /**
     * Removes any URI query from the given uri and return a new URI that does not contain the
     * query portion.
     *
     * @param uri
     *        The URI whose query value is to be removed.
     *
     * @return a new URI that does not contain a query value.
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static URI removeQuery(URI uri) throws URISyntaxException {
        return PropertyUtil.replaceQuery(uri, (String) null);
    }

    /**
     * Given a URI parse and extract any URI query options and return them as a Key / Value
     * mapping.
     *
     * This method handles composite URI types and will extract the URI options
     * from the outermost composite URI.
     *
     * @param uri
     *        The URI whose query should be extracted and processed.
     *
     * @return A Mapping of the URI options.
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static Map<String, String> parseParameters(URI uri) throws URISyntaxException {
        if (!isCompositeURI(uri)) {
            if (uri.getRawQuery() == null) {
                return Collections.emptyMap();
            } else {
                try {
                    return PropertyUtil.parseQuery(PropertyUtil.stripPrefix(uri.getRawQuery(), "?"));
                } catch (Exception e) {
                    throw new URISyntaxException(uri.toString(), e.getMessage());
                }
            }
        } else {
            CompositeData data = URISupport.parseComposite(uri);
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.putAll(data.getParameters());
            if (parameters.isEmpty()) {
                parameters = Collections.emptyMap();
            }

            return parameters;
        }
    }

    /**
     * Given a Key / Value mapping create and append a URI query value that represents the
     * mapped entries, return the newly updated URI that contains the value of the given URI and
     * the appended query value.  The values in the parameters map should not be URL encoded values
     * as this method will perform an encode on them resulting in double encoded values.
     *
     * @param uri
     *        The source URI that will have the Map entries appended as a URI query value.
     * @param queryParameters
     *        The Key / Value mapping that will be transformed into a URI query string.
     *
     * @return A new URI value that combines the given URI and the constructed query string.
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static URI applyParameters(URI uri, Map<String, String> queryParameters) throws URISyntaxException {
        return applyParameters(uri, queryParameters, "");
    }

    /**
     * Given a Key / Value mapping create and append a URI query value that represents the
     * mapped entries, return the newly updated URI that contains the value of the given URI and
     * the appended query value.  Only values in the given options map that start with the provided
     * prefix are appended to the provided URI, the prefix is stripped off before the insertion.
     * <P>
     * This method replaces the value of any matching query string options in the original URI with
     * the value given in the provided query parameters map.
     *
     * @param uri
     *        The source URI that will have the Map entries appended as a URI query value.
     * @param queryParameters
     *        The Key / Value mapping that will be transformed into a URI query string.
     * @param optionPrefix
     *        A string value that when not null or empty is used to prefix each query option
     *        key.
     *
     * @return A new URI value that combines the given URI and the constructed query string.
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static URI applyParameters(URI uri, Map<String, String> queryParameters, String optionPrefix) throws URISyntaxException {
        if (queryParameters != null && !queryParameters.isEmpty()) {

            Map<String, String> currentParameters = new LinkedHashMap<String, String>(parseParameters(uri));

            // Replace any old values with the new value from the provided map.
            for (Map.Entry<String, String> param : queryParameters.entrySet()) {
                if (param.getKey().startsWith(optionPrefix)) {
                    final String key = param.getKey().substring(optionPrefix.length());
                    currentParameters.put(key, param.getValue());
                }
            }

            uri = PropertyUtil.replaceQuery(uri, currentParameters);
        }

        return uri;
    }
}
