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

    private static final Pattern VARIABLE_OR_ESCAPE_PATTERN = Pattern.compile("(?:\\$\\{([^\\}]*):-([^\\}]*)\\})|(?:\\$\\{([^\\}]*)\\})|(?:\\$(\\$))");
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
     * @throws UnresolvedVariableException
     *             If a variable without a default can't be expanded because it (or indirectly its value) is not resolvable.
     * @throws IllegalArgumentException
     *             if an argument can't be expanded due to other issue with the input.
     * @throws NullPointerException
     *             if a resolver is not supplied
     */
    public static final String expand(String input, Resolver resolver) throws UnresolvedVariableException, IllegalArgumentException, NullPointerException {
        if(resolver == null) {
            throw new NullPointerException("Resolver must be supplied");
        }

        if (input == null) {
            return null;
        }

        return expand0(input, resolver, new Stack<String>());
    }

    private static final String expand0(String input, Resolver resolver, Stack<String> stack) {
        Matcher matcher = VARIABLE_OR_ESCAPE_PATTERN.matcher(input);

        StringBuffer result = null;
        while (matcher.find()) {
            if(result == null) {
                result = new StringBuffer();
            }

            processMatch(resolver, stack, matcher, result);
        }

        if(result == null) {
            // No matches found, return the original input
            return input;
        } else {
            matcher.appendTail(result);

            return result.toString();
        }
    }

    private static void processMatch(Resolver resolver, Stack<String> stack, Matcher matcher, StringBuffer result) {
        String var = matcher.group(1); // Variable match, with a default
        if(var != null) {
            String resolved = null;
            try {
                resolved = resolve(var, resolver, stack);
            } catch(UnresolvedVariableException uve) {
                resolved = matcher.group(2); // The default
            }

            matcher.appendReplacement(result, Matcher.quoteReplacement(resolved));
        } else {
            var = matcher.group(3); // Variable match, no default
            if (var != null) {
                matcher.appendReplacement(result, Matcher.quoteReplacement(resolve(var, resolver, stack)));
            } else {
                String esc = matcher.group(4); // Escape matcher
                if (ESCAPE.equals(esc)) {
                    matcher.appendReplacement(result, Matcher.quoteReplacement(ESCAPE));
                } else {
                    throw new IllegalArgumentException(esc);
                }
            }
        }
    }

    private static final String resolve(String var, Resolver resolver, Stack<String> stack) {
        if (stack.contains(var)) {
            throw new IllegalArgumentException(String.format("Recursively defined variable '%s', stack=%s", var, stack));
        }

        String result = resolver.resolve(var);
        if (result == null) {
            throw new UnresolvedVariableException("Unable to resolve variable: " + var);
        }

        stack.push(var);
        try {
            return expand0(result, resolver, stack);
        } finally {
            stack.pop();
        }
    }

    public static final class UnresolvedVariableException extends IllegalArgumentException {

        private static final long serialVersionUID = -4256718043645749788L;

        public UnresolvedVariableException() {
            super();
        }

        public UnresolvedVariableException(String s) {
            super(s);
        }
    }
}