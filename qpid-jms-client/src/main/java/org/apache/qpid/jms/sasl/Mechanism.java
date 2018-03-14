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
import java.util.Map;

import javax.security.sasl.SaslException;

/**
 * Interface for all SASL authentication mechanism implementations.
 */
public interface Mechanism extends Comparable<Mechanism> {

    /**
     * Relative priority values used to arrange the found SASL
     * mechanisms in a preferred order where the level of security
     * generally defines the preference.
     */
    public enum PRIORITY {
        LOWEST(0),
        LOWER_STILL(1),
        LOWER(2),
        LOW(3),
        MEDIUM(4),
        HIGH(5),
        HIGHER(6),
        HIGHER_STILL(7),
        HIGHEST(8);

        private final int value;

        private PRIORITY(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
       }
    };

    /**
     * @return return the relative priority of this SASL mechanism.
     */
    int getPriority();

    /**
     * @return the well known name of this SASL mechanism.
     */
    String getName();

    /**
     * Perform any configuration initiation required by the mechanism.
     *
     * @param options
     *        An immutable map of sasl options. Will always be non-null.
     */
    void init(Map<String, String> options);

    /**
     * Create an initial response based on selected mechanism.
     *
     * May be null if there is no initial response.
     *
     * @return the initial response, or null if there isn't one.
     * @throws SaslException if an error occurs computing the response.
     */
    byte[] getInitialResponse() throws SaslException;

    /**
     * Create a response based on a given challenge from the remote peer.
     *
     * @param challenge
     *        the challenge that this Mechanism should response to.
     *
     * @return the response that answers the given challenge.
     * @throws SaslException if an error occurs computing the response.
     */
    byte[] getChallengeResponse(byte[] challenge) throws SaslException;

    /**
     * Verifies that the SASL exchange has completed successfully. This is
     * an opportunity for the mechanism to ensure that all mandatory
     * steps have been completed successfully and to cleanup and resources
     * that are held by this Mechanism.
     *
     * @throws SaslException if the outcome of the SASL exchange is not valid for this Mechanism
     */
    void verifyCompletion() throws SaslException;

    /**
     * Sets the user name value for this Mechanism.  The Mechanism can ignore this
     * value if it does not utilize user name in it's authentication processing.
     *
     * @param username
     *        The user name given.
     */
    void setUsername(String username);

    /**
     * Returns the configured user name value for this Mechanism.
     *
     * @return the currently set user name value for this Mechanism.
     */
    String getUsername();

    /**
     * Sets the password value for this Mechanism.  The Mechanism can ignore this
     * value if it does not utilize a password in it's authentication processing.
     *
     * @param username
     *        The user name given.
     */
    void setPassword(String username);

    /**
     * Returns the configured password value for this Mechanism.
     *
     * @return the currently set password value for this Mechanism.
     */
    String getPassword();

    /**
     * Allows the mechanism to determine if it can be used given the authentication
     * provided.
     *
     * @param username
     * 		The user name given to the client for authentication.
     * @param password
     * 		The password given to the client for authentication.
     * @param localPrincipal
     * 		The local Principal configured for the client for authentication.
     *
     * @return if this Mechanism is able to validate using the given credentials.
     */
    boolean isApplicable(String username, String password, Principal localPrincipal);

    /**
     * Allows the mechanism to indicate if it is enabled by default, or only when explicitly enabled
     * through configuring the permitted sasl mechanisms.
     *
     * @return true if this Mechanism is enabled by default.
     */
    boolean isEnabledByDefault();

    /**
     * Allows a mechanism to report additional information on the reason for
     * authentication failure (e.g. provided in a challenge from the server)
     *
     * @return information on the reason for failure, or null if no such information is available
     */
    default String getAdditionalFailureInformation() {
        return null;
    }
}
