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

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.net.ssl.SSLContext;

/**
 * Utilities for properties
 */
public class PropertyUtil {

    private static final String UTF8 = "UTF-8";

    /**
     * Creates a URI from the original URI and the given parameters.
     *
     * The string values in the Map will be URL Encoded by this method which means that if an
     * already encoded value is passed it will be double encoded resulting in corrupt values
     * in the newly created URI.
     *
     * @param originalURI
     *        The URI whose current parameters are removed and replaced with the given remainder value.
     * @param properties
     *        The URI properties that should be used to replace the current ones in the target.
     *
     * @return a new URI that matches the original one but has its query options replaced with
     *         the given ones.
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static URI replaceQuery(URI originalURI, Map<String, String> properties) throws URISyntaxException {
        String queryString = createQueryString(properties);
        if (queryString.length() == 0) {
            queryString = null;
        }

        return replaceQuery(originalURI, queryString);
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
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static URI replaceQuery(URI uri, String query) throws URISyntaxException {
        String uriAsString = uri.toString();

        String fragment = null;

        // Strip existing fragment if any
        int hashMark = uriAsString.lastIndexOf("#");
        // make sure hash mark is not within parentheses
        if (hashMark < uriAsString.lastIndexOf(")")) {
            hashMark = -1;
        }
        if (hashMark > 0) {
            fragment = uriAsString.substring(hashMark);
            uriAsString = uriAsString.substring(0, hashMark);
        }

        // strip existing query if any
        int questionMark = uriAsString.lastIndexOf("?");
        // make sure question mark is not within parentheses
        if (questionMark < uriAsString.lastIndexOf(")")) {
            questionMark = -1;
        }
        if (questionMark > 0) {
            uriAsString = uriAsString.substring(0, questionMark);
        }

        if (query != null && query.length() > 0) {
            uriAsString += "?" + query;
        }

        if (fragment != null && fragment.length() > 0) {
            uriAsString += fragment;
        }

        return new URI(uriAsString);
    }

    /**
     * Creates a URI with the given query, removing an previous query value from the given URI.
     *
     * @param uri
     *        The source URI whose existing query is replaced with the newly supplied one.
     *
     * @return a new URI that is a combination of the original URI and the given query string.
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static URI eraseQuery(URI uri) throws URISyntaxException {
        return replaceQuery(uri, (String) null);
    }

    /**
     * Given a key / value mapping, create and return a URI formatted query string that is valid
     * and can be appended to a URI string.  The values in the given Map are URL Encoded during
     * construction of the query string, if the original values were previously encoded this can
     * result in double encoding.
     *
     * @param options
     *        The Mapping that will create the new Query string.
     *
     * @return a URI formatted query string.
     *
     * @throws URISyntaxException if the given URI is invalid.
     */
    public static String createQueryString(Map<String, ? extends Object> options) throws URISyntaxException {
        try {
            if (options.size() > 0) {
                StringBuffer rc = new StringBuffer();
                boolean first = true;
                for (Entry<String, ? extends Object> entry : options.entrySet()) {
                    if (first) {
                        first = false;
                    } else {
                        rc.append("&");
                    }

                    rc.append(URLEncoder.encode(entry.getKey(), UTF8));
                    rc.append("=");
                    rc.append(URLEncoder.encode((String) entry.getValue(), UTF8));
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
     * Get properties from the Query portion of the given URI.
     *
     * @param uri
     *        the URI whose Query string should be parsed.
     *
     * @return <Code>Map</Code> of properties from the parsed string.
     *
     * @throws Exception if an error occurs while parsing the query options.
     */
    public static Map<String, String> parseQuery(URI uri) throws Exception {
        if (uri == null) {
            return Collections.emptyMap();
        }

        return parseQuery(uri.getRawQuery());
    }

    /**
     * Get properties from a URI Query String obtained by calling the getRawQuery method
     * on a given URI or from some other source.
     *
     * @param queryString
     *        the query string obtained from called getRawQuery on a given URI.
     *
     * @return <Code>Map</Code> of properties from the parsed string.
     *
     * @throws Exception if an error occurs while parsing the query options.
     */
    public static Map<String, String> parseQuery(String queryString) throws Exception {
        if (queryString != null && !queryString.isEmpty()) {
            Map<String, String> rc = new LinkedHashMap<String, String>();
            String[] parameters = queryString.split("&");
            for (int i = 0; i < parameters.length; i++) {
                int p = parameters[i].indexOf("=");
                if (p >= 0) {
                    String name = URLDecoder.decode(parameters[i].substring(0, p), UTF8);
                    String value = URLDecoder.decode(parameters[i].substring(p + 1), UTF8);

                    rc.put(name, value);
                } else {
                    rc.put(parameters[i], null);
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
    public static Map<String, String> filterProperties(Map<String, String> properties, String optionPrefix) {
        if (properties == null) {
            throw new IllegalArgumentException("The given properties object was null.");
        }

        Map<String, String> rc = new LinkedHashMap<String, String>(properties.size());

        for (Iterator<Entry<String, String>> iter = properties.entrySet().iterator(); iter.hasNext();) {
            Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith(optionPrefix)) {
                String name = entry.getKey().substring(optionPrefix.length());
                rc.put(name, entry.getValue());
                iter.remove();
            }
        }

        return rc;
    }

    /**
     * Set properties on an object using the provided map. The return value
     * indicates if all properties from the given map were set on the target object.
     *
     * @param target
     *        the object whose properties are to be set from the map options.
     * @param properties
     *        the properties that should be applied to the given object.
     *
     * @return true if all values in the properties map were applied to the target object.
     */
    public static Map<String, String> setProperties(Object target, Map<String, String> properties) {
        if (target == null) {
            throw new IllegalArgumentException("target object cannot be null");
        }
        if (properties == null) {
            throw new IllegalArgumentException("Given Properties object cannot be null");
        }

        Map<String, String> unmatched = new LinkedHashMap<String, String>();

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!setProperty(target, entry.getKey(), entry.getValue())) {
                unmatched.put(entry.getKey(), entry.getValue());
            }
        }

        return Collections.unmodifiableMap(unmatched);
    }

    /**
     * Set properties on an object using the provided Properties object. The return value
     * indicates if all properties from the given map were set on the target object.
     *
     * @param target
     *        the object whose properties are to be set from the map options.
     * @param properties
     *        the properties that should be applied to the given object.
     *
     * @return an unmodifiable map with any values that could not be applied to the target.
     */
    public static Map<String, Object> setProperties(Object target, Properties properties) {
        if (target == null) {
            throw new IllegalArgumentException("target object cannot be null");
        }
        if (properties == null) {
            throw new IllegalArgumentException("Given Properties object cannot be null");
        }

        Map<String, Object> unmatched = new LinkedHashMap<String, Object>();

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (!setProperty(target, (String) entry.getKey(), entry.getValue())) {
                unmatched.put((String) entry.getKey(), entry.getValue());
            }
        }

        return Collections.<String, Object>unmodifiableMap(unmatched);
    }

    /**
     * Get properties from an object using reflection.  If the passed object is null an
     * empty <code>Map</code> is returned.
     *
     * @param object
     *        the Object whose properties are to be extracted.
     *
     * @return <Code>Map</Code> of properties extracted from the given object.
     *
     * @throws Exception if an error occurs while examining the object's properties.
     */
    public static Map<String, String> getProperties(Object object) throws Exception {
        if (object == null) {
            return Collections.emptyMap();
        }

        Map<String, String> properties = new LinkedHashMap<String, String>();
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
                            properties.put(pd.getName(), ("" + value));
                        } else if (value instanceof SSLContext) {
                            // ignore this one..
                        } else {
                            Map<String, String> inner = getProperties(value);
                            for (Map.Entry<String, String> entry : inner.entrySet()) {
                                properties.put(pd.getName() + "." + entry.getKey(), entry.getValue());
                            }
                        }
                    }
                }
            }
        }

        return properties;
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
     * Set a property named property on a given Object.
     * <p>
     * The object is searched for an set method that would match the given named
     * property and if one is found.  If necessary an attempt will be made to convert
     * the new value to an acceptable type.
     *
     * @param target
     *        The object whose property is to be set.
     * @param name
     *        The name of the property to set.
     * @param value
     *        The new value to set for the named property.
     *
     * @return true if the property was able to be set on the target object.
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
            List<Method> setters = findSetterMethod(clazz, name);
            if (setters == null || setters.isEmpty()) {
                return false;
            }

            Throwable failure = null;
            for (Method setter : setters) {
                // If the type is null or it matches the needed type, just use the
                // value directly
                if (value == null || value.getClass() == setter.getParameterTypes()[0]) {
                    try {
                        setter.invoke(target, new Object[] { value });
                        failure = null;
                        break;
                    } catch (Throwable error) {
                        failure = error;
                    }
                } else {
                    try {
                        setter.invoke(target, new Object[] { convert(value, setter.getParameterTypes()[0]) });
                        failure = null;
                        break;
                    } catch (Throwable error) {
                        failure = error;
                    }
                }
            }

            return failure == null;
        } catch (Throwable ignore) {
            return false;
        }
    }

    /**
     * Return a String minus the given prefix.  If the string does not start
     * with the given prefix the original string value is returned.
     *
     * @param value
     *        The String whose prefix is to be removed.
     * @param prefix
     *        The prefix string to remove from the target string.
     *
     * @return stripped version of the original input string.
     */
    public static String stripPrefix(String value, String prefix) {
        if (value != null && prefix != null && value.startsWith(prefix)) {
            return value.substring(prefix.length());
        }
        return value;
    }

    /**
     * Return a portion of a String value by looking beyond the given
     * character.
     *
     * @param value
     *        The string value to split
     * @param c
     *        The character that marks the split point.
     *
     * @return the sub-string value starting beyond the given character.
     */
    public static String stripUpto(String value, char c) {
        String result = null;
        if (value != null) {
            int index = value.indexOf(c);
            if (index > 0) {
                result = value.substring(index + 1);
            }
        }
        return result;
    }

    /**
     * Return a String up to and including character
     *
     * @param value
     *        The string value to split
     * @param c
     *        The character that marks the start of split point.
     *
     * @return the sub-string value starting from the given character.
     */
    public static String stripBefore(String value, char c) {
        String result = value;
        if (value != null) {
            int index = value.indexOf(c);
            if (index > 0) {
                result = value.substring(0, index);
            }
        }
        return result;
    }

    private static List<Method> findSetterMethod(Class<? extends Object> clazz, String name) {
        List<Method> matches = new ArrayList<>();

        // Build the method name.
        name = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
        Method[] methods = clazz.getMethods();
        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            Class<? extends Object> params[] = method.getParameterTypes();
            if (method.getName().equals(name) && params.length == 1) {
                matches.add(method);
            }
        }

        return matches;
    }

    private static Object convert(Object value, Class<?> type) throws Exception {
        if (value == null) {
            if (boolean.class.isAssignableFrom(type)) {
                return Boolean.FALSE;
            }
            return null;
        }

        if (type.isAssignableFrom(value.getClass())) {
            return type.cast(value);
        }

        // special for String[] as we do not want to use a PropertyEditor for that
        if (type.isAssignableFrom(String[].class)) {
            return StringArrayConverter.convertToStringArray(value);
        }

        if (type == URI.class) {
            return new URI(value.toString());
        }

        return TypeConversionSupport.convert(value, type);
    }
}
