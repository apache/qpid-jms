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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.Principal;

import org.junit.jupiter.api.Test;

public class XOauth2MechanismTest {

    @Test
    public void testGetInitialResponseWithNullUserAndPassword() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        byte[] response = mech.getInitialResponse();
        assertNotNull(response);
        assertTrue(response.length > 0);
    }

    @Test
    public void testGetChallengeResponse() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        byte[] response = mech.getChallengeResponse(new byte[1]);
        assertNotNull(response);
        assertTrue(response.length == 0);
    }

    @Test
    public void testGetChallengeResponsePopulatesAdditionalDiagnosticInformation() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        byte[] response = mech.getChallengeResponse("additional data".getBytes(StandardCharsets.UTF_8));
        assertNotNull(response);
        assertTrue(response.length == 0);
        assertEquals("additional data", mech.getAdditionalFailureInformation());
    }

    @Test
    public void testIsNotApplicableWithNoCredentials() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse(mech.isApplicable(null, null, null), "Should not be applicable with no credentials");
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse(mech.isApplicable(null, "pass", null), "Should not be applicable with no username");
    }

    @Test
    public void testIsNotApplicableWithNoToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse(mech.isApplicable("user", null, null), "Should not be applicable with no token");
    }

    @Test
    public void testIsNotApplicableWithEmtpyUser() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse(mech.isApplicable("", "pass", null), "Should not be applicable with empty username");
    }

    @Test
    public void testIsNotApplicableWithEmtpyToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse(mech.isApplicable("user", "", null), "Should not be applicable with empty token");
    }

    /** RFC6749 defines the OAUTH2 an access token as comprising VSCHAR elements (\x20-7E) */
    @Test
    public void testIsNotApplicableWithIllegalAccessToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse(mech.isApplicable("user", "illegalChar\000", null), "Should not be applicable with non vschars");
    }


    @Test
    public void testIsNotApplicableWithEmtpyUserAndToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse(mech.isApplicable("", "", null), "Should not be applicable with empty user and token");
    }

    @Test
    public void testIsApplicableWithUserAndToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertTrue(mech.isApplicable("user", "2YotnFZFEjr1zCsicMWpAA", null), "Should be applicable with user and token");
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertTrue(mech.isApplicable("user", "2YotnFZFEjr1zCsicMWpAA", new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        }), "Should be applicable with user and token and principal");
    }

    @Test
    public void testIsEnabledByDefault() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertTrue(mech.isEnabledByDefault(), "Should be enabled by default");
    }
}
