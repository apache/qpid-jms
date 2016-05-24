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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generator for Globally unique Strings used to identify resources within a given Connection.
 */
public class IdGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(IdGenerator.class);

    private static String hostName;

    private final String prefix;
    private final AtomicLong sequence = new AtomicLong(1);

    public static final String DEFAULT_PREFIX = "ID:";
    public static final String PROPERTY_IDGENERATOR_HOST_PREFIX = "qpidjms.idgenerator.hostPrefixEnabled";

    static {
        boolean canAccessSystemProps = true;
        try {
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkPropertiesAccess();
            }
        } catch (SecurityException se) {
            canAccessSystemProps = false;
        }

        if (canAccessSystemProps) {
            boolean addHostPrefix = Boolean.getBoolean(PROPERTY_IDGENERATOR_HOST_PREFIX);
            if (addHostPrefix) {
                try {
                    LOG.trace("ID Generator attemtping to lookup host name prefix.");
                    hostName = InetAddressUtil.getLocalHostName();
                } catch (Exception e) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("could not generate host name prefix from DNS lookup.", e);
                    } else {
                        LOG.warn("could not generate host name prefix from DNS lookup: {} {}", e.getClass().getCanonicalName(), e.getMessage());
                    }

                    // Restore interrupted state so higher level code can deal with it.
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }

                if (hostName == null) {
                    hostName = "localhost";
                }

                hostName = sanitizeHostName(hostName);
            }
        }
    }

    /**
     * Construct an IdGenerator using the given prefix value as the initial
     * prefix entry for all Ids generated (default is 'ID:').
     *
     * @param prefix
     *      The prefix value that is applied to all generated IDs.
     */
    public IdGenerator(String prefix) {
        this.prefix = prefix + (hostName != null ? (hostName + ":") : "");
    }

    /**
     * Construct an IdGenerator using the default prefix value.
     */
    public IdGenerator() {
        this(DEFAULT_PREFIX);
    }

    /**
     * Generate a unique id using the configured characteristics.
     *
     * @return a newly generated unique id value.
     */
    public String generateId() {
        StringBuilder sb = new StringBuilder(64);

        sb.append(prefix);
        sb.append(UUID.randomUUID());
        sb.append(":");
        sb.append(sequence.getAndIncrement());

        return sb.toString();
    }

    //----- Internal implementation ------------------------------------------//

    protected static String sanitizeHostName(String hostName) {
        boolean changed = false;

        StringBuilder sb = new StringBuilder();
        for (char ch : hostName.toCharArray()) {
            // only include ASCII chars
            if (ch < 127) {
                sb.append(ch);
            } else {
                changed = true;
            }
        }

        if (changed) {
            String newHost = sb.toString();
            LOG.info("Sanitized hostname from: {} to: {}", hostName, newHost);
            return newHost;
        } else {
            return hostName;
        }
    }
}
