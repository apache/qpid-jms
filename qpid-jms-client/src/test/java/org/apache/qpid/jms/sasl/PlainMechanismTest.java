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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.Principal;

import org.junit.Test;

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

        assertFalse("Should not be applicable with no credentials", mech.isApplicable(null, null, null));
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse("Should not be applicable with no username", mech.isApplicable(null, "pass", null));
    }

    @Test
    public void testIsNotApplicableWithNoPassword() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse("Should not be applicable with no password", mech.isApplicable("user", null, null));
    }

    @Test
    public void testIsNotApplicableWithEmtpyUser() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse("Should not be applicable with empty username", mech.isApplicable("", "pass", null));
    }

    @Test
    public void testIsNotApplicableWithEmtpyPassword() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse("Should not be applicable with empty password", mech.isApplicable("user", "", null));
    }

    @Test
    public void testIsNotApplicableWithEmtpyUserAndPassword() {
        PlainMechanism mech = new PlainMechanism();

        assertFalse("Should not be applicable with empty user and password", mech.isApplicable("", "", null));
    }

    @Test
    public void testIsApplicableWithUserAndPassword() {
        PlainMechanism mech = new PlainMechanism();

        assertTrue("Should be applicable with user and password", mech.isApplicable("user", "password", null));
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        PlainMechanism mech = new PlainMechanism();

        assertTrue("Should be applicable with user and password and principal", mech.isApplicable("user", "password", new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        }));
    }

    @Test
    public void testIsEnabledByDefault() {
        PlainMechanism mech = new PlainMechanism();

        assertTrue("Should be enabled by default", mech.isEnabledByDefault());
    }
}
