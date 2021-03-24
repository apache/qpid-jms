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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

/**
 * The known good used by these tests was generated using external tooling.
 */
public class ScramSHA512MechanismTest extends AbstractScramSHAMechanismTestBase {

    private static final String USERNAME = "user";
    private static final String PASSWORD = "pencil";

    private static final String CLIENT_NONCE = "rOprNGfwEbeRWgbNEkqO";

    private static final byte[] EXPECTED_CLIENT_INITIAL_RESPONSE = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVER_FIRST_MESSAGE = "r=rOprNGfwEbeRWgbNEkqO02431b08-2f89-4bad-a4e6-80c0564ec865,s=Yin2FuHTt/M0kJWb0t9OI32n2VmOGi3m+JfjOvuDF88=,i=4096".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EXPECTED_CLIENT_FINAL_MESSAGE = "c=biws,r=rOprNGfwEbeRWgbNEkqO02431b08-2f89-4bad-a4e6-80c0564ec865,p=Hc5yec3NmCD7t+kFRw4/3yD6/F3SQHc7AVYschRja+Bc3sbdjlA0eH1OjJc0DD4ghn1tnXN5/Wr6qm9xmaHt4A==".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVER_FINAL_MESSAGE = "v=BQuhnKHqYDwQWS5jAw4sZed+C9KFUALsbrq81bB0mh+bcUUbbMPNNmBIupnS2AmyyDnG5CTBQtkjJ9kyY4kzmw==".getBytes(StandardCharsets.UTF_8);

    public ScramSHA512MechanismTest() {
        super(EXPECTED_CLIENT_INITIAL_RESPONSE,
              SERVER_FIRST_MESSAGE,
              EXPECTED_CLIENT_FINAL_MESSAGE,
              SERVER_FINAL_MESSAGE);
    }

    @Override
    protected Mechanism getConfiguredMechanism() {

        Mechanism mech = new ScramSHA512Mechanism(CLIENT_NONCE);
        mech.setUsername(USERNAME);
        mech.setPassword(PASSWORD);
        return mech;
    }

    @Test
    public void testDifferentClientNonceOnEachInstance() throws Exception {
        ScramSHA512Mechanism mech = new ScramSHA512Mechanism();
        mech.setUsername(USERNAME);
        mech.setPassword(PASSWORD);

        ScramSHA512Mechanism mech2 = new ScramSHA512Mechanism();
        mech2.setUsername(USERNAME);
        mech2.setPassword(PASSWORD);

        byte[] clientInitialResponse = mech.getInitialResponse();
        byte[] clientInitialResponse2 = mech2.getInitialResponse();

        assertTrue(new String(clientInitialResponse, StandardCharsets.UTF_8).startsWith("n,,n=user,r="));
        assertTrue(new String(clientInitialResponse2, StandardCharsets.UTF_8).startsWith("n,,n=user,r="));

        assertThat(clientInitialResponse, not(equalTo(clientInitialResponse2)));
    }

    @Test
    public void testIsEnabledByDefault() {
        ScramSHA512Mechanism mech = new ScramSHA512Mechanism();

        assertTrue("Should be enabled by default", mech.isEnabledByDefault());
    }

    @Test
    public void testUsernameCommaEqualsCharactersEscaped() throws Exception {
        String originalUsername = "user,name=";
        String escapedUsername = "user=2Cname=3D";

        String expectedInitialResponseString = "n,,n=" + escapedUsername + ",r=" + CLIENT_NONCE;

        ScramSHA512Mechanism mech = new ScramSHA512Mechanism(CLIENT_NONCE);
        mech.setUsername(originalUsername);
        mech.setPassword("password");

        byte[] clientInitialResponse = mech.getInitialResponse();
        assertArrayEquals(expectedInitialResponseString.getBytes(StandardCharsets.UTF_8), clientInitialResponse);
    }

    @Test
    public void testPasswordCommaEqualsCharactersNotEscaped() throws Exception {
        Mechanism mechanism = getConfiguredMechanism();
        mechanism.setPassword(PASSWORD + ",=");

        byte[] clientInitialResponse = mechanism.getInitialResponse();
        assertArrayEquals(EXPECTED_CLIENT_INITIAL_RESPONSE, clientInitialResponse);

        byte[] serverFirstMessage = "r=rOprNGfwEbeRWgbNEkqOf0f492bc-13cc-4050-8461-59f74f24e989,s=g2nOdJkyb5SlvqLbJb6S5+ckZpYFJ+AkJqxlmDAZYbY=,i=4096".getBytes(StandardCharsets.UTF_8);
        byte[] expectedClientFinalMessage = "c=biws,r=rOprNGfwEbeRWgbNEkqOf0f492bc-13cc-4050-8461-59f74f24e989,p=vxWDY/qwIhNPGnYvGKxRESmP9nP4bmOSssNLVN6sWo1cAatr3HAxIogJ9qe2kxLdrmQcyCkW7sgq+8ybSgPphQ==".getBytes(StandardCharsets.UTF_8);

        byte[] clientFinalMessage = mechanism.getChallengeResponse(serverFirstMessage);

        assertArrayEquals(expectedClientFinalMessage, clientFinalMessage);

        byte[] serverFinalMessage = "v=l/icAMt3q4ym4Yh7syjjekFZ3r3L3+l+e08WmS3m3pMXCXhPf865+9bfRRprO6xPhFWKyuD+PPh+jQf8JBVojQ==".getBytes(StandardCharsets.UTF_8);
        byte[] expectedFinalChallengeResponse = "".getBytes();

        assertArrayEquals(expectedFinalChallengeResponse, mechanism.getChallengeResponse(serverFinalMessage));

        mechanism.verifyCompletion();
    }
}