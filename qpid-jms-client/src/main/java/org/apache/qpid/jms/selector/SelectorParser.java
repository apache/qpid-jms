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
package org.apache.qpid.jms.selector;

import java.io.StringReader;
import java.util.Collections;
import java.util.Map;

import org.apache.qpid.jms.selector.filter.BooleanExpression;
import org.apache.qpid.jms.selector.filter.FilterException;
import org.apache.qpid.jms.selector.parser.SelectorParserImpl;
import org.apache.qpid.jms.util.LRUCache;

public class SelectorParser {

    private static final Map<String, Object> cache = Collections.synchronizedMap(new LRUCache<String, Object>(100));

    public static BooleanExpression parse(String sql) throws FilterException {
        Object result = cache.get(sql);
        if (result instanceof FilterException) {
            throw (FilterException) result;
        } else if (result instanceof BooleanExpression) {
            return (BooleanExpression) result;
        } else {
            try {
                BooleanExpression e = null;
                SelectorParserImpl parser = new SelectorParserImpl(new StringReader(sql));
                e = parser.JmsSelector();
                cache.put(sql, e);
                return e;
            } catch (Throwable e) {
                FilterException fe = new FilterException(sql, e);
                cache.put(sql, fe);
                throw fe;
            }
        }
    }

    public static void clearCache() {
        cache.clear();
    }
}
