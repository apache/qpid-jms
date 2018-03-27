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

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.security.Principal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

        assertFalse("Should not be applicable with no credentials", mech.isApplicable(null, null, null));
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse("Should not be applicable with no username", mech.isApplicable(null, "pass", null));
    }

    @Test
    public void testIsNotApplicableWithNoToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse("Should not be applicable with no token", mech.isApplicable("user", null, null));
    }

    @Test
    public void testIsNotApplicableWithEmtpyUser() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse("Should not be applicable with empty username", mech.isApplicable("", "pass", null));
    }

    @Test
    public void testIsNotApplicableWithEmtpyToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse("Should not be applicable with empty token", mech.isApplicable("user", "", null));
    }

    /** RFC6749 defines the OAUTH2 an access token as comprising VSCHAR elements (\x20-7E) */
    @Test
    public void testIsNotApplicableWithIllegalAccessToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse("Should not be applicable with non vschars", mech.isApplicable("user", "illegalChar\000", null));
    }


    @Test
    public void testIsNotApplicableWithEmtpyUserAndToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertFalse("Should not be applicable with empty user and token", mech.isApplicable("", "", null));
    }

    @Test
    public void testIsApplicableWithUserAndToken() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertTrue("Should be applicable with user and token", mech.isApplicable("user", "2YotnFZFEjr1zCsicMWpAA", null));
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertTrue("Should be applicable with user and token and principal", mech.isApplicable("user", "2YotnFZFEjr1zCsicMWpAA", new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        }));
    }

    @Test
    public void testIsEnabledByDefault() {
        XOauth2Mechanism mech = new XOauth2Mechanism();

        assertTrue("Should be enabled by default", mech.isEnabledByDefault());
    }
}
