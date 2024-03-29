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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;

import org.junit.jupiter.api.Test;

public class AnonymousMechanismTest {

    @Test
    public void testGetInitialResponseWithNullUserAndPassword() {
        AnonymousMechanism mech = new AnonymousMechanism();

        byte[] response = mech.getInitialResponse();
        assertNotNull(response);
        assertTrue(response.length == 0);
    }

    @Test
    public void testGetChallengeResponse() {
        AnonymousMechanism mech = new AnonymousMechanism();

        byte[] response = mech.getChallengeResponse(new byte[1]);
        assertNotNull(response);
        assertTrue(response.length == 0);
    }

    @Test
    public void testIsApplicableWithNoCredentials() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isApplicable(null, null, null), "Should be applicable with no credentials");
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isApplicable(null, "pass", null), "Should be applicable with no username");
    }

    @Test
    public void testIsApplicableWithNoPassword() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isApplicable("user", null, null), "Should be applicable with no password");
    }

    @Test
    public void testIsApplicableWithEmtpyUser() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isApplicable("", "pass", null), "Should be applicable with empty username");
    }

    @Test
    public void testIsApplicableWithEmtpyPassword() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isApplicable("user", "", null), "Should be applicable with empty password");
    }

    @Test
    public void testIsApplicableWithEmtpyUserAndPassword() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isApplicable("", "", null), "Should be applicable with empty user and password");
    }

    @Test
    public void testIsApplicableWithUserAndPassword() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isApplicable("user", "password", null), "Should be applicable with user and password");
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isApplicable("user", "password", new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        }), "Should be applicable with user and password and principal");
    }

    @Test
    public void testIsEnabledByDefault() {
        AnonymousMechanism mech = new AnonymousMechanism();

        assertTrue(mech.isEnabledByDefault(), "Should be enabled by default");
    }
}
