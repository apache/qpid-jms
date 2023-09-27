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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;

import org.junit.jupiter.api.Test;

public class PlainMechanismTest {

    @Test
    public void testGetInitialResponseWithNullUserAndPassword() {
        PlainMechanism mech = new PlainMechanism();

        byte[] response = mech.getInitialResponse();
        assertNotNull(response);
        assertTrue(response.length > 0);
    }

    @Test
    public void testGetChallengeResponse() {
        PlainMechanism mech = new PlainMechanism();

        byte[] response = mech.getChallengeResponse(new byte[1]);
        assertNotNull(response);
        assertTrue(response.length == 0);
    }

    @Test
    public void testIsNotApplicableWithNoCredentials() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse(mech.isApplicable(null, null, null), "Should not be applicable with no credentials");
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse(mech.isApplicable(null, "pass", null), "Should not be applicable with no username");
    }

    @Test
    public void testIsNotApplicableWithNoPassword() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse(mech.isApplicable("user", null, null), "Should not be applicable with no password");
    }

    @Test
    public void testIsNotApplicableWithEmtpyUser() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse(mech.isApplicable("", "pass", null), "Should not be applicable with empty username");
    }

    @Test
    public void testIsNotApplicableWithEmtpyPassword() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse(mech.isApplicable("user", "", null), "Should not be applicable with empty password");
    }

    @Test
    public void testIsNotApplicableWithEmtpyUserAndPassword() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse(mech.isApplicable("", "", null), "Should not be applicable with empty user and password");
    }

    @Test
    public void testIsApplicableWithUserAndPassword() {
        PlainMechanism mech = new PlainMechanism();

        assertTrue(mech.isApplicable("user", "password", null), "Should be applicable with user and password");
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        PlainMechanism mech = new PlainMechanism();

        assertTrue(mech.isApplicable("user", "password", new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        }), "Should be applicable with user and password and principal");
    }

    @Test
    public void testIsEnabledByDefault() {
        PlainMechanism mech = new PlainMechanism();

        assertTrue(mech.isEnabledByDefault(), "Should be enabled by default");
    }
}
