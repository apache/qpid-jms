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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ToStringSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ToStringSupport.class);

    private ToStringSupport() {
    }

    public static String toString(Object target) {
        return toString(target, Object.class, null);
    }

    @SuppressWarnings({ "rawtypes" })
    public static String toString(Object target, Class stopClass) {
        return toString(target, stopClass, null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static String toString(Object target, Class stopClass, Map<String, Object> overrideFields) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
        addFields(target, target.getClass(), stopClass, map);
        if (overrideFields != null) {
            for(String key : overrideFields.keySet()) {
                Object value = overrideFields.get(key);
                map.put(key, value);
            }

        }
        StringBuffer buffer = new StringBuffer(simpleName(target.getClass()));
        buffer.append(" {");
        Set<Entry<String, Object>> entrySet = map.entrySet();
        boolean first = true;
        for (Map.Entry<String,Object> entry : entrySet) {
            Object value = entry.getValue();
            Object key = entry.getKey();
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(key);
            buffer.append(" = ");

            appendToString(buffer, key, value);
        }
        buffer.append("}");
        return buffer.toString();
    }

    protected static void appendToString(StringBuffer buffer, Object key, Object value) {
        if (key.toString().toLowerCase(Locale.ENGLISH).contains("password")){
            buffer.append("*****");
        } else {
            buffer.append(value);
        }
    }

    public static String simpleName(Class<?> clazz) {
        String name = clazz.getName();
        int p = name.lastIndexOf(".");
        if (p >= 0) {
            name = name.substring(p + 1);
        }
        return name;
    }

    @SuppressWarnings({ "rawtypes" })
    private static void addFields(Object target, Class startClass, Class<Object> stopClass, LinkedHashMap<String, Object> map) {

        if (startClass != stopClass) {
            addFields(target, startClass.getSuperclass(), stopClass, map);
        }

        Field[] fields = startClass.getDeclaredFields();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())
                || Modifier.isPrivate(field.getModifiers())) {
                continue;
            }

            try {
                field.setAccessible(true);
                Object o = field.get(target);
                if (o != null && o.getClass().isArray()) {
                    try {
                        o = Arrays.asList((Object[])o);
                    } catch (Exception e) {
                    }
                }
                map.put(field.getName(), o);
            } catch (Exception e) {
                LOG.debug("Error getting field " + field + " on class " + startClass + ". This exception is ignored.", e);
            }
        }
    }
}
