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
 * Default implementation of the deserialization policy that can read allow and deny lists of
 * classes/packages from the environment, and be updated by the connection uri options.
 *
 * The policy reads a default deny list string value (comma separated) from the system property
 * {@value #DENYLIST_PROPERTY} which defaults to null which indicates an empty deny list.
 *
 * The policy reads a default allow list string value (comma separated) from the system property
 * {@value #ALLOWLIST_PROPERTY} which defaults to a {@value #CATCH_ALL_WILDCARD} which
 * indicates that all classes are allowed.
 *
 * The deny list overrides the allow list, entries that could match both are counted as denied.
 *
 * If the policy should treat all classes as untrusted the deny list should be set to
 * {@value #CATCH_ALL_WILDCARD}".
 */
public class JmsDefaultDeserializationPolicy implements JmsDeserializationPolicy {

    /**
     * Value used to indicate that all classes should be allowed or denied,
     */
    public static final String CATCH_ALL_WILDCARD = "*";

    /**
     * @deprecated new applications should use the ALLOWLIST_PROPERTY instead
     */
    @Deprecated
    public static final String DEPRECATED_ALLOWLIST_PROPERTY = "org.apache.qpid.jms.deserialization.white_list";

    /**
     * @deprecated new applications should use the DENYLIST_PROPERTY instead
     */
    @Deprecated
    public static final String DEPRECATED_DENYLIST_PROPERTY = "org.apache.qpid.jms.deserialization.black_list";

    public static final String ALLOWLIST_PROPERTY = "org.apache.qpid.jms.deserialization.allow_list";
    public static final String DENYLIST_PROPERTY = "org.apache.qpid.jms.deserialization.deny_list";

    private List<String> allowList = new ArrayList<String>();
    private List<String> denyList = new ArrayList<String>();

    /**
     * Creates an instance of this policy with default configuration.
     */
    public JmsDefaultDeserializationPolicy() {

        // TODO: Upon removal of deprecated constants replace with call to use the CATCH_ALL_WILDCARD as the default
        //        final String allowList = System.getProperty(ALLOWLIST_PROPERTY, CATCH_ALL_WILDCARD);

        final String deprecatedAllowList = System.getProperty(DEPRECATED_ALLOWLIST_PROPERTY, CATCH_ALL_WILDCARD);
        final String allowList = System.getProperty(ALLOWLIST_PROPERTY, deprecatedAllowList);

        setAllowList(allowList);

        // TODO: Upon removal of deprecated constants replace with call to use the no default value method
        //        final String denyList = System.getProperty(DENYLIST_PROPERTY);

        final String deprecatedDenyList = System.getProperty(DEPRECATED_DENYLIST_PROPERTY);
        final String denyList = System.getProperty(DENYLIST_PROPERTY, deprecatedDenyList);

        setDenyList(denyList);
    }

    /**
     * @param source
     *      The instance whose configuration should be copied from.
     */
    public JmsDefaultDeserializationPolicy(JmsDefaultDeserializationPolicy source) {
        this.allowList.addAll(source.allowList);
        this.denyList.addAll(source.denyList);
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

        for (String denyListEntry : denyList) {
            if (CATCH_ALL_WILDCARD.equals(denyListEntry)) {
                return false;
            } else if (isClassOrPackageMatch(className, denyListEntry)) {
                return false;
            }
        }

        for (String allowListEntry : allowList) {
            if (CATCH_ALL_WILDCARD.equals(allowListEntry)) {
                return true;
            } else if (isClassOrPackageMatch(className, allowListEntry)) {
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
     * @return the allow list configured on this policy instance.
     *
     * @deprecated Use the replacement method {@link #getAllowList()}
     */
    @Deprecated
    public String getWhiteList() {
        return getAllowList();
    }

    /**
     * @return the allow list configured on this policy instance.
     */
    public String getAllowList() {
        Iterator<String> entries = allowList.iterator();
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
     * @return the deny list configured on this policy instance.
     *
     * @deprecated Use the replacement method {@link #getDenyList()}
     */
    @Deprecated
    public String getBlackList() {
        return getDenyList();
    }

    /**
     * @return the deny list configured on this policy instance.
     */
    public String getDenyList() {
        Iterator<String> entries = denyList.iterator();
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
     * @param allowList
     *      the allow list that this policy is configured to recognize.
     *
     * @deprecated Use the replacement method {@link #setAllowList(String)}
     */
    @Deprecated
    public void setWhiteList(String allowList) {
        setAllowList(allowList);
    }

    /**
     * Replaces the currently configured allow list with a comma separated
     * string containing the new allow list. Null or empty string denotes
     * no allow list entries, {@value #CATCH_ALL_WILDCARD} indicates that
     * all classes are allowed.
     *
     * @param allowList
     *      the allow list that this policy is configured to recognize.
     */
    public void setAllowList(String allowList) {
        ArrayList<String> list = new ArrayList<String>();
        if (allowList != null && !allowList.isEmpty()) {
            list.addAll(Arrays.asList(allowList.split(",")));
        }

        this.allowList = list;
    }

    /**
     * @param denyList
     *      the deny list that this policy is configured to recognize.
     *
     * @deprecated Use the replacement method {@link #setDenyList(String)}
     */
    @Deprecated
    public void setBlackList(String denyList) {
        setDenyList(denyList);
    }

    /**
     * Replaces the currently configured deny list with a comma separated
     * string containing the new deny list. Null or empty string denotes
     * no deny list entries, {@value #CATCH_ALL_WILDCARD} indicates that
     * all classes are denied.
     *
     * @param denyList
     *      the deny list that this policy is configured to recognize.
     */
    public void setDenyList(String denyList) {
        ArrayList<String> list = new ArrayList<String>();
        if (denyList != null && !denyList.isEmpty()) {
            list.addAll(Arrays.asList(denyList.split(",")));
        }

        this.denyList = list;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((allowList == null) ? 0 : allowList.hashCode());
        result = prime * result + ((denyList == null) ? 0 : denyList.hashCode());
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

        if (allowList == null) {
            if (other.allowList != null) {
                return false;
            }
        } else if (!allowList.equals(other.allowList)) {
            return false;
        }

        if (denyList == null) {
            if (other.denyList != null) {
                return false;
            }
        } else if (!denyList.equals(other.denyList)) {
            return false;
        }

        return true;
    }
}
