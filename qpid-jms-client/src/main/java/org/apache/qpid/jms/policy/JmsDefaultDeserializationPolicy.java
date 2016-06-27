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
package org.apache.qpid.jms.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.qpid.jms.JmsDestination;

/**
 * Default implementation of the deserialization policy that can read white and black list of
 * classes/packages from the environment, and be updated by the connection uri options.
 *
 * The policy reads a default blackList string value (comma separated) from the system property
 * {@value #BLACKLIST_PROPERTY} which defaults to null which indicates an empty blacklist.
 *
 * The policy reads a default whitelist string value (comma separated) from the system property
 * {@value #WHITELIST_PROPERTY} which defaults to a {@value #CATCH_ALL_WILDCARD} which
 * indicates that all classes are whitelisted.
 *
 * The blacklist overrides the whitelist, entries that could match both are counted as blacklisted.
 *
 * If the policy should treat all classes as untrusted the blacklist should be set to
 * {@value #CATCH_ALL_WILDCARD}".
 */
public class JmsDefaultDeserializationPolicy implements JmsDeserializationPolicy {

    /**
     * Value used to indicate that all classes should be white or black listed,
     */
    public static final String CATCH_ALL_WILDCARD = "*";

    public static final String WHITELIST_PROPERTY = "org.apache.qpid.jms.deserialization.white_list";
    public static final String BLACKLIST_PROPERTY = "org.apache.qpid.jms.deserialization.black_list";

    private List<String> whiteList = new ArrayList<String>();
    private List<String> blackList = new ArrayList<String>();

    /**
     * Creates an instance of this policy with default configuration.
     */
    public JmsDefaultDeserializationPolicy() {
        String whitelist = System.getProperty(WHITELIST_PROPERTY, CATCH_ALL_WILDCARD);
        setWhiteList(whitelist);

        String blackList = System.getProperty(BLACKLIST_PROPERTY);
        setBlackList(blackList);
    }

    /**
     * @param source
     *      The instance whose configuration should be copied from.
     */
    public JmsDefaultDeserializationPolicy(JmsDefaultDeserializationPolicy source) {
        this.whiteList.addAll(source.whiteList);
        this.blackList.addAll(source.blackList);
    }

    @Override
    public JmsDeserializationPolicy copy() {
        return new JmsDefaultDeserializationPolicy(this);
    }

    @Override
    public boolean isTrustedType(JmsDestination destination, Class<?> clazz) {
        if (clazz == null) {
            return true;
        }

        String className = clazz.getCanonicalName();
        if (className == null) {
            // Shouldn't happen as we pre-processed things, but just in case..
            className = clazz.getName();
        }

        for (String blackListEntry : blackList) {
            if (CATCH_ALL_WILDCARD.equals(blackListEntry)) {
                return false;
            } else if (isClassOrPackageMatch(className, blackListEntry)) {
                return false;
            }
        }

        for (String whiteListEntry : whiteList) {
            if (CATCH_ALL_WILDCARD.equals(whiteListEntry)) {
                return true;
            } else if (isClassOrPackageMatch(className, whiteListEntry)) {
                return true;
            }
        }

        // Failing outright rejection or allow from above, reject.
        return false;
    }

    private final boolean isClassOrPackageMatch(String className, String listEntry) {
        if (className == null) {
            return false;
        }

        // Check if class is an exact match of the entry
        if (className.equals(listEntry)) {
            return true;
        }

        // Check if class is from a [sub-]package matching the entry
        int entryLength = listEntry.length();
        if (className.length() > entryLength && className.startsWith(listEntry) && '.' == className.charAt(entryLength)) {
            return true;
        }

        return false;
    }

    /**
     * @return the whiteList configured on this policy instance.
     */
    public String getWhiteList() {
        Iterator<String> entries = whiteList.iterator();
        StringBuilder builder = new StringBuilder();

        while (entries.hasNext()) {
            builder.append(entries.next());
            if (entries.hasNext()) {
                builder.append(",");
            }
        }

        return builder.toString();
    }

    /**
     * @return the blackList configured on this policy instance.
     */
    public String getBlackList() {
        Iterator<String> entries = blackList.iterator();
        StringBuilder builder = new StringBuilder();

        while (entries.hasNext()) {
            builder.append(entries.next());
            if (entries.hasNext()) {
                builder.append(",");
            }
        }

        return builder.toString();
    }

    /**
     * Replaces the currently configured whiteList with a comma separated
     * string containing the new whiteList. Null or empty string denotes
     * no whiteList entries, {@value #CATCH_ALL_WILDCARD} indicates that
     * all classes are whiteListed.
     *
     * @param whiteList
     *      the whiteList that this policy is configured to recognize.
     */
    public void setWhiteList(String whiteList) {
        ArrayList<String> list = new ArrayList<String>();
        if (whiteList != null && !whiteList.isEmpty()) {
            list.addAll(Arrays.asList(whiteList.split(",")));
        }

        this.whiteList = list;
    }

    /**
     * Replaces the currently configured blackList with a comma separated
     * string containing the new blackList. Null or empty string denotes
     * no blacklist entries, {@value #CATCH_ALL_WILDCARD} indicates that
     * all classes are blacklisted.
     *
     * @param blackList
     *      the blackList that this policy is configured to recognize.
     */
    public void setBlackList(String blackList) {
        ArrayList<String> list = new ArrayList<String>();
        if (blackList != null && !blackList.isEmpty()) {
            list.addAll(Arrays.asList(blackList.split(",")));
        }

        this.blackList = list;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((whiteList == null) ? 0 : whiteList.hashCode());
        result = prime * result + ((blackList == null) ? 0 : blackList.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        JmsDefaultDeserializationPolicy other = (JmsDefaultDeserializationPolicy) obj;

        if (whiteList == null) {
            if (other.whiteList != null) {
                return false;
            }
        } else if (!whiteList.equals(other.whiteList)) {
            return false;
        }

        if (blackList == null) {
            if (other.blackList != null) {
                return false;
            }
        } else if (!blackList.equals(other.blackList)) {
            return false;
        }

        return true;
    }
}
