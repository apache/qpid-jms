/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.util;

import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class VariableExpansion {

    @FunctionalInterface
    public interface Resolver {
        String resolve(String name);
    }

    private static final Pattern VARIABLE_OR_ESCAPE_PATTERN = Pattern.compile("(?:\\$\\{([^\\}]*)\\})|(?:\\$(\\$))");
    private static final String ESCAPE = "$";

    private VariableExpansion() {
        // No instances
    }

    //================== Helper Resolvers ==================

    public static final Resolver ENV_VAR_RESOLVER = prop -> System.getenv(prop);

    public static final Resolver SYS_PROP_RESOLVER = prop -> System.getProperty(prop);

    public static final class MapResolver implements Resolver {
        private final Map<String, String> map;

        public MapResolver(Map<String, String> map) {
            this.map = map;
        }

        public String resolve(String name) {
            return map.get(name);
        }
    }

    //======================= Methods ======================

    /**
     * Expands any variables found in the given input string.
     *
     * @param input
     *            the string to expand any variables in
     * @param resolver
     *            the resolver to use
     * @return the expanded output
     *
     * @throws IllegalArgumentException
     *             if an argument can't be expanded, e.g because a variable is not resolvable.
     * @throws NullPointerException
     *             if a resolver is not supplied
     */
    public static final String expand(String input, Resolver resolver) throws IllegalArgumentException, NullPointerException {
        if(resolver == null) {
            throw new NullPointerException("Resolver must be supplied");
        }

        if (input == null) {
            return null;
        }

        return expand(input, resolver, new Stack<String>());
    }

    private static final String expand(String input, Resolver resolver, Stack<String> stack) {
        Matcher matcher = VARIABLE_OR_ESCAPE_PATTERN.matcher(input);

        StringBuffer result = null;
        while (matcher.find()) {
            if(result == null) {
                result = new StringBuffer();
            }

            String var = matcher.group(1); // Variable match
            if (var != null) {
                matcher.appendReplacement(result, Matcher.quoteReplacement(resolve(var, resolver, stack)));
            } else {
                String esc = matcher.group(2); // Escape matcher
                if (ESCAPE.equals(esc)) {
                    matcher.appendReplacement(result, Matcher.quoteReplacement(ESCAPE));
                } else {
                    throw new IllegalArgumentException(esc);
                }
            }
        }

        if(result == null) {
            // No match found, return the original input
            return input;
        }

        matcher.appendTail(result);

        return result.toString();
    }

    private static final String resolve(String var, Resolver resolver, Stack<String> stack) {
        if (stack.contains(var)) {
            throw new IllegalArgumentException(String.format("Recursively defined variable '%s', stack=%s", var, stack));
        }

        String result = resolver.resolve(var);
        if (result == null) {
            throw new IllegalArgumentException("Unable to resolve variable: " + var);
        }

        stack.push(var);
        try {
            return expand(result, resolver, stack);
        } finally {
            stack.pop();
        }
    }
}