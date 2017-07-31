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

public class ExternalMechanismTest {

    @Test
    public void testGetInitialResponseWithNullUserAndPassword() {
        ExternalMechanism mech = new ExternalMechanism();

        byte[] response = mech.getInitialResponse();
        assertNotNull(response);
        assertTrue(response.length == 0);
    }

    @Test
    public void testGetChallengeResponse() {
        ExternalMechanism mech = new ExternalMechanism();

        byte[] response = mech.getChallengeResponse(new byte[1]);
        assertNotNull(response);
        assertTrue(response.length == 0);
    }

    @Test
    public void testIsNotApplicableWithUserAndPasswordButNoPrincipal() {
        ExternalMechanism mech = new ExternalMechanism();

        assertFalse("Should not be applicable with user and password but no principal", mech.isApplicable("user", "password", null));
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        ExternalMechanism mech = new ExternalMechanism();

        assertTrue("Should be applicable with user and password and principal", mech.isApplicable("user", "password", new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        }));
    }

    @Test
    public void testIsApplicableWithPrincipalOnly() {
        ExternalMechanism mech = new ExternalMechanism();

        assertTrue("Should be applicable with principal only", mech.isApplicable(null, null, new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        }));
    }

    @Test
    public void testIsEnabledByDefault() {
        ExternalMechanism mech = new ExternalMechanism();

        assertTrue("Should be enabled by default", mech.isEnabledByDefault());
    }
}
