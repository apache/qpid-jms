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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.security.Principal;
import java.util.Base64;

import org.junit.Test;

import javax.security.sasl.SaslException;

/**
 * The known good used by these tests is taken from the example in RFC 2195 section 2.
 */

public class CramMD5MechanismTest {

    private final byte[] SERVER_FIRST_MESSAGE = Base64.getDecoder().decode("PDE4OTYuNjk3MTcwOTUyQHBvc3RvZmZpY2UucmVzdG9uLm1jaS5uZXQ+");
    private final byte[] EXPECTED_CLIENT_FINAL_MESSAGE = Base64.getDecoder().decode("dGltIGI5MTNhNjAyYzdlZGE3YTQ5NWI0ZTZlNzMzNGQzODkw");
    private static final String USERNAME = "tim";
    private static final String PASSWORD = "tanstaaftanstaaf";

    @Test
    public void testSuccessfulAuthentication() throws Exception {
        Mechanism mechanism = new CramMD5Mechanism();
        mechanism.setUsername(USERNAME);
        mechanism.setPassword(PASSWORD);

        byte[] clientInitialResponse = mechanism.getInitialResponse();
        assertNull(clientInitialResponse);

        byte[] clientFinalResponse = mechanism.getChallengeResponse(SERVER_FIRST_MESSAGE);
        assertArrayEquals(EXPECTED_CLIENT_FINAL_MESSAGE, clientFinalResponse);

        mechanism.verifyCompletion();
    }

    @Test
    public void testIsNotApplicableWithNoCredentials() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertFalse("Should not be applicable with no credentials", mech.isApplicable(null, null, null));
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertFalse("Should not be applicable with no username", mech.isApplicable(null, "pass", null));
    }

    @Test
    public void testIsNotApplicableWithNoPassword() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertFalse("Should not be applicable with no password", mech.isApplicable("user", null, null));
    }

    @Test
    public void testIsNotApplicableWithEmtpyUser() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertFalse("Should not be applicable with empty username", mech.isApplicable("", "pass", null));
    }

    @Test
    public void testIsNotApplicableWithEmtpyPassword() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertFalse("Should not be applicable with empty password", mech.isApplicable("user", "", null));
    }

    @Test
    public void testIsNotApplicableWithEmtpyUserAndPassword() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertFalse("Should not be applicable with empty user and password", mech.isApplicable("", "", null));
    }

    @Test
    public void testIsApplicableWithUserAndPassword() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertTrue("Should be applicable with user and password", mech.isApplicable("user", "password", null));
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertTrue("Should be applicable with user and password and principal", mech.isApplicable("user", "password", new Principal() {
            @Override
            public String getName() {
                return "name";
            }
        }));
    }

    @Test
    public void testIsEnabledByDefault() {
        CramMD5Mechanism mech = new CramMD5Mechanism();

        assertTrue("Should be enabled by default", mech.isEnabledByDefault());
    }

    public void testIncompleteExchange() throws Exception {
        Mechanism mechanism = new CramMD5Mechanism();

        mechanism.getInitialResponse();

        try {
            mechanism.verifyCompletion();
            fail("Exception not thrown");
        } catch (SaslException e) {
            // PASS
        }
    }
}
