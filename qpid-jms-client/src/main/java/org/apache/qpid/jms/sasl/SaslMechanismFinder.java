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
package org.apache.qpid.jms.sasl;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.qpid.jms.util.FactoryFinder;
import org.apache.qpid.jms.util.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to find a SASL Mechanism that most closely matches the preferred set
 * of Mechanisms supported by the remote peer.
 *
 * The Matching mechanism is chosen by first find all instances of SASL
 * mechanism types that are supported on the remote peer, and then making a
 * final selection based on the Mechanism in the found set that has the
 * highest priority value.
 */
public class SaslMechanismFinder {

    private static final Logger LOG = LoggerFactory.getLogger(SaslMechanismFinder.class);

    private static final FactoryFinder<MechanismFactory> MECHANISM_FACTORY_FINDER =
        new FactoryFinder<MechanismFactory>(MechanismFactory.class,
            "META-INF/services/" + SaslMechanismFinder.class.getPackage().getName().replace(".", "/") + "/");

    /**
     * Attempts to find a matching Mechanism implementation given a list of supported
     * mechanisms from a remote peer.  Can return null if no matching Mechanisms are
     * found.
     *
     * @param username
     *        the username, or null if there is none
     * @param password
     *        the password, or null if there is none
     * @param localPrincipal
     *        the Principal associated with the transport, or null if there is none
     * @param mechRestrictions
     *        The possible mechanism(s) to which the client should restrict its
     *        mechanism selection to if offered by the server, or null if there
     *        is no restriction
     * @param remoteMechanisms
     *        list of mechanism names that are supported by the remote peer.
     *
     * @return the best matching Mechanism for the supported remote set.
     */
    public static Mechanism findMatchingMechanism(String username, String password, Principal localPrincipal, Set<String> mechRestrictions, String... remoteMechanisms) {

        Mechanism match = null;
        List<Mechanism> found = new ArrayList<Mechanism>();

        for (String remoteMechanism : remoteMechanisms) {
            MechanismFactory factory = findMechanismFactory(remoteMechanism);
            if (factory != null) {
                Mechanism mech = factory.createMechanism();

                boolean mechConfigured = mechRestrictions != null && mechRestrictions.contains(remoteMechanism);
                if(mechRestrictions != null && !mechConfigured) {
                    LOG.debug("Skipping {} mechanism because it is not in the configured mechanisms restriction set", remoteMechanism);
                } else if(mech.isApplicable(username, password, localPrincipal)) {
                    if(mech.isEnabledByDefault() || mechConfigured) {
                        found.add(mech);
                    } else {
                        LOG.debug("Skipping {} mechanism as it must be explicitly enabled in the configured sasl mechanisms", mech);
                    }
                } else {
                    LOG.debug("Skipping {} mechanism because the available credentials are not sufficient", mech);
                }
            }
        }

        if (!found.isEmpty()) {
            // Sorts by priority using Mechanism comparison and return the last value in
            // list which is the Mechanism deemed to be the highest priority match.
            Collections.sort(found);
            match = found.get(found.size() - 1);
        }

        LOG.info("Best match for SASL auth was: {}", match);

        return match;
    }

    /**
     * Searches for a MechanismFactory by using the scheme from the given name.
     *
     * The search first checks the local cache of mechanism factories before moving on
     * to search in the classpath.
     *
     * @param name
     *        The name of the authentication mechanism to search for.
     *
     * @return a mechanism factory instance matching the name, or null if none was created.
     */
    protected static MechanismFactory findMechanismFactory(String name) {
        if (name == null || name.isEmpty()) {
            LOG.warn("No SASL mechanism name was specified");
            return null;
        }

        MechanismFactory factory = null;
        try {
            factory = MECHANISM_FACTORY_FINDER.newInstance(name);
        } catch (ResourceNotFoundException rnfe) {
            LOG.debug("Unknown SASL mechanism: [" + name + "]");
        } catch (Exception e) {
            LOG.warn("Caught exception while finding factory for SASL mechanism {}: {}", name, e.getMessage());
        }

        return factory;
    }
}
