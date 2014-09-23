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

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

/**
 * Utilities for properties
 */
public class PropertyUtil {

    /**
     * Creates a URI from the original URI and the given parameters.
     *
     * @param originalURI
     *        The URI whose current parameters are remove and replaced with the given remainder
     *        value.
     * @param params
     *        The URI params that should be used to replace the current ones in the target.
     *
     * @return a new URI that matches the original one but has its query options replaced with
     *         the given ones.
     *
     * @throws URISyntaxException
     */
    public static URI replaceQuery(URI originalURI, Map<String, String> params) throws URISyntaxException {
        String s = createQueryString(params);
        if (s.length() == 0) {
            s = null;
        }
        return replaceQuery(originalURI, s);
    }

    /**
     * Creates a URI with the given query, removing an previous query value from the given URI.
     *
     * @param uri
     *        The source URI whose existing query is replaced with the newly supplied one.
     * @param query
     *        The new URI query string that should be appended to the given URI.
     *
     * @return a new URI that is a combination of the original URI and the given query string.
     * @throws URISyntaxException
     */
    public static URI replaceQuery(URI uri, String query) throws URISyntaxException {
        String schemeSpecificPart = uri.getRawSchemeSpecificPart();
        // strip existing query if any
        int questionMark = schemeSpecificPart.lastIndexOf("?");
        // make sure question mark is not within parentheses
        if (questionMark < schemeSpecificPart.lastIndexOf(")")) {
            questionMark = -1;
        }
        if (questionMark > 0) {
            schemeSpecificPart = schemeSpecificPart.substring(0, questionMark);
        }
        if (query != null && query.length() > 0) {
            schemeSpecificPart += "?" + query;
        }
        return new URI(uri.getScheme(), schemeSpecificPart, uri.getFragment());
    }

    /**
     * Creates a URI with the given query, removing an previous query value from the given URI.
     *
     * @param uri
     *        The source URI whose existing query is replaced with the newly supplied one.
     * @param query
     *        The new URI query string that should be appended to the given URI.
     *
     * @return a new URI that is a combination of the original URI and the given query string.
     * @throws URISyntaxException
     */
    public static URI eraseQuery(URI uri) throws URISyntaxException {
        return replaceQuery(uri, (String) null);
    }

    /**
     * Given a key / value mapping, create and return a URI formatted query string that is valid
     * and can be appended to a URI.
     *
     * @param options
     *        The Mapping that will create the new Query string.
     *
     * @return a URI formatted query string.
     *
     * @throws URISyntaxException
     */
    public static String createQueryString(Map<String, ? extends Object> options) throws URISyntaxException {
        try {
            if (options.size() > 0) {
                StringBuffer rc = new StringBuffer();
                boolean first = true;
                for (String key : options.keySet()) {
                    if (first) {
                        first = false;
                    } else {
                        rc.append("&");
                    }
                    String value = (String) options.get(key);
                    rc.append(URLEncoder.encode(key, "UTF-8"));
                    rc.append("=");
                    rc.append(URLEncoder.encode(value, "UTF-8"));
                }
                return rc.toString();
            } else {
                return "";
            }
        } catch (UnsupportedEncodingException e) {
            throw (URISyntaxException) new URISyntaxException(e.toString(), "Invalid encoding").initCause(e);
        }
    }

    /**
     * Get properties from a URI
     *
     * @param uri
     * @return <Code>Map</Code> of properties
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> parseParameters(URI uri) throws Exception {
        return uri.getQuery() == null ? Collections.EMPTY_MAP : parseQuery(stripPrefix(uri.getQuery(), "?"));
    }

    /**
     * Parse properties from a named resource -eg. a URI or a simple name e.g.
     * foo?name="fred"&size=2
     *
     * @param uri
     * @return <Code>Map</Code> of properties
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> parseParameters(String uri) throws Exception {
        return uri == null ? Collections.EMPTY_MAP : parseQuery(stripUpto(uri, '?'));
    }

    /**
     * Get properties from a uri
     *
     * @param uri
     * @return <Code>Map</Code> of properties
     *
     * @throws Exception
     */
    public static Map<String, String> parseQuery(String uri) throws Exception {
        if (uri != null) {
            Map<String, String> rc = new HashMap<String, String>();
            if (uri != null) {
                String[] parameters = uri.split("&");
                for (int i = 0; i < parameters.length; i++) {
                    int p = parameters[i].indexOf("=");
                    if (p >= 0) {
                        String name = URLDecoder.decode(parameters[i].substring(0, p), "UTF-8");
                        String value = URLDecoder.decode(parameters[i].substring(p + 1), "UTF-8");
                        rc.put(name, value);
                    } else {
                        rc.put(parameters[i], null);
                    }
                }
            }
            return rc;
        }
        return Collections.emptyMap();
    }

    /**
     * Given a map of properties, filter out only those prefixed with the given value, the
     * values filtered are returned in a new Map instance.
     *
     * @param properties
     *        The map of properties to filter.
     * @param optionPrefix
     *        The prefix value to use when filtering.
     *
     * @return a filter map with only values that match the given prefix.
     */
    public static Map<String, String> filterProperties(Map<String, String> props, String optionPrefix) {
        if (props == null) {
            throw new IllegalArgumentException("props was null.");
        }

        HashMap<String, String> rc = new HashMap<String, String>(props.size());

        for (Iterator<String> iter = props.keySet().iterator(); iter.hasNext();) {
            String name = iter.next();
            if (name.startsWith(optionPrefix)) {
                String value = props.get(name);
                name = name.substring(optionPrefix.length());
                rc.put(name, value);
                iter.remove();
            }
        }

        return rc;
    }

    /**
     * Add bean properties to a URI
     *
     * @param uri
     * @param bean
     * @return <Code>Map</Code> of properties
     * @throws Exception
     */
    public static String addPropertiesToURIFromBean(String uri, Object bean) throws Exception {
        Map<String, String> props = PropertyUtil.getProperties(bean);
        return PropertyUtil.addPropertiesToURI(uri, props);
    }

    /**
     * Add properties to a URI
     *
     * @param uri
     * @param props
     * @return uri with properties on
     * @throws Exception
     */
    public static String addPropertiesToURI(URI uri, Map<String, String> props) throws Exception {
        return addPropertiesToURI(uri.toString(), props);
    }

    /**
     * Add properties to a URI
     *
     * @param uri
     * @param props
     * @return uri with properties on
     * @throws Exception
     */
    public static String addPropertiesToURI(String uri, Map<String, String> props) throws Exception {
        String result = uri;
        if (uri != null && props != null) {
            StringBuilder base = new StringBuilder(stripBefore(uri, '?'));
            Map<String, String> map = parseParameters(uri);
            if (!map.isEmpty()) {
                map.putAll(props);
            }
            if (!map.isEmpty()) {
                base.append('?');
                boolean first = true;
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    if (!first) {
                        base.append('&');
                    }
                    first = false;
                    base.append(entry.getKey()).append("=").append(entry.getValue());
                }
                result = base.toString();
            }
        }
        return result;
    }

    /**
     * Set properties on an object using the provided map. The return value indicates if all
     * properties from the given map were set on the target object.
     *
     * @param target
     *        the object whose properties are to be set from the map options.
     * @param props
     *        the properties that should be applied to the given object.
     *
     * @return true if all values in the props map were applied to the target object.
     */
    public static boolean setProperties(Object target, Map<String, String> props) {
        if (target == null) {
            throw new IllegalArgumentException("target was null.");
        }
        if (props == null) {
            throw new IllegalArgumentException("props was null.");
        }

        int setCounter = 0;

        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (setProperty(target, entry.getKey(), entry.getValue())) {
                setCounter++;
            }
        }

        return setCounter == props.size();
    }

    /**
     * Get properties from an object
     *
     * @param object
     * @return <Code>Map</Code> of properties
     * @throws Exception
     */
    public static Map<String, String> getProperties(Object object) throws Exception {
        Map<String, String> props = new LinkedHashMap<String, String>();
        BeanInfo beanInfo = Introspector.getBeanInfo(object.getClass());
        Object[] NULL_ARG = {};
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        if (propertyDescriptors != null) {
            for (int i = 0; i < propertyDescriptors.length; i++) {
                PropertyDescriptor pd = propertyDescriptors[i];
                if (pd.getReadMethod() != null && !pd.getName().equals("class") && !pd.getName().equals("properties") && !pd.getName().equals("reference")) {
                    Object value = pd.getReadMethod().invoke(object, NULL_ARG);
                    if (value != null) {
                        if (value instanceof Boolean || value instanceof Number || value instanceof String || value instanceof URI || value instanceof URL) {
                            props.put(pd.getName(), ("" + value));
                        } else if (value instanceof SSLContext) {
                            // ignore this one..
                        } else {
                            Map<String, String> inner = getProperties(value);
                            for (Map.Entry<String, String> entry : inner.entrySet()) {
                                props.put(pd.getName() + "." + entry.getKey(), entry.getValue());
                            }
                        }
                    }
                }
            }
        }
        return props;
    }

    /**
     * Find a specific property getter in a given object based on a property name.
     *
     * @param object
     *        the object to search.
     * @param name
     *        the property name to search for.
     *
     * @return the result of invoking the specific property get method.
     *
     * @throws Exception if an error occurs while searching the object's bean info.
     */
    public static Object getProperty(Object object, String name) throws Exception {
        BeanInfo beanInfo = Introspector.getBeanInfo(object.getClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        if (propertyDescriptors != null) {
            for (int i = 0; i < propertyDescriptors.length; i++) {
                PropertyDescriptor pd = propertyDescriptors[i];
                if (pd.getReadMethod() != null && pd.getName().equals(name)) {
                    return pd.getReadMethod().invoke(object);
                }
            }
        }
        return null;
    }

    /**
     * Set a property
     *
     * @param target
     * @param name
     * @param value
     * @return true if set
     */
    public static boolean setProperty(Object target, String name, Object value) {
        try {
            int dotPos = name.indexOf(".");
            while (dotPos >= 0) {
                String getterName = name.substring(0, dotPos);
                target = getProperty(target, getterName);
                name = name.substring(dotPos + 1);
                dotPos = name.indexOf(".");
            }

            Class<? extends Object> clazz = target.getClass();
            Method setter = findSetterMethod(clazz, name);
            if (setter == null) {
                return false;
            }
            // If the type is null or it matches the needed type, just use the
            // value directly
            if (value == null || value.getClass() == setter.getParameterTypes()[0]) {
                setter.invoke(target, new Object[] { value });
            } else {
                // We need to convert it
                setter.invoke(target, new Object[] { convert(value, setter.getParameterTypes()[0]) });
            }
            return true;
        } catch (Throwable ignore) {
            return false;
        }
    }

    /**
     * Return a String past a prefix
     *
     * @param value
     * @param prefix
     * @return stripped
     */
    public static String stripPrefix(String value, String prefix) {
        if (value.startsWith(prefix)) {
            return value.substring(prefix.length());
        }
        return value;
    }

    /**
     * Return a String from to a character
     *
     * @param value
     * @param c
     * @return stripped
     */
    public static String stripUpto(String value, char c) {
        String result = null;
        int index = value.indexOf(c);
        if (index > 0) {
            result = value.substring(index + 1);
        }
        return result;
    }

    /**
     * Return a String up to and including character
     *
     * @param value
     * @param c
     * @return stripped
     */
    public static String stripBefore(String value, char c) {
        String result = value;
        int index = value.indexOf(c);
        if (index > 0) {
            result = value.substring(0, index);
        }
        return result;
    }

    private static Method findSetterMethod(Class<? extends Object> clazz, String name) {
        // Build the method name.
        name = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
        Method[] methods = clazz.getMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            Class<? extends Object> params[] = method.getParameterTypes();
            if (method.getName().equals(name) && params.length == 1) {
                return method;
            }
        }
        return null;
    }

    private static Object convert(Object value, Class<?> type) throws Exception {
        PropertyEditor editor = PropertyEditorManager.findEditor(type);
        if (editor != null) {
            editor.setAsText(value.toString());
            return editor.getValue();
        }
        if (type == URI.class) {
            return new URI(value.toString());
        }
        return null;
    }
}
