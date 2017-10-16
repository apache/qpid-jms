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
 * The known good used by these tests is taken from the example in RFC 5802 section 5.
 */
public class ScramSHA1MechanismTest extends AbstractScramSHAMechanismTestBase {

    private static final String USERNAME = "user";
    private static final String PASSWORD = "pencil";

    private static final String CLIENT_NONCE = "fyko+d2lbbFgONRv9qkxdawL";

    private static final byte[] EXPECTED_CLIENT_INITIAL_RESPONSE = "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVER_FIRST_MESSAGE = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EXPECTED_CLIENT_FINAL_MESSAGE = "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVER_FINAL_MESSAGE = "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=".getBytes(StandardCharsets.UTF_8);

    public ScramSHA1MechanismTest() {
        super(EXPECTED_CLIENT_INITIAL_RESPONSE,
              SERVER_FIRST_MESSAGE,
              EXPECTED_CLIENT_FINAL_MESSAGE,
              SERVER_FINAL_MESSAGE);
    }

    @Override
    protected Mechanism getConfiguredMechanism() {

        Mechanism mech = new ScramSHA1Mechanism(CLIENT_NONCE);
        mech.setUsername(USERNAME);
        mech.setPassword(PASSWORD);
        return mech;
    }

    @Test
    public void testDifferentClientNonceOnEachInstance() throws Exception {
        ScramSHA1Mechanism mech = new ScramSHA1Mechanism();
        mech.setUsername(USERNAME);
        mech.setPassword(PASSWORD);

        ScramSHA1Mechanism mech2 = new ScramSHA1Mechanism();
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
        ScramSHA1Mechanism mech = new ScramSHA1Mechanism();

        assertTrue("Should be enabled by default", mech.isEnabledByDefault());
    }

    @Test
    public void testUsernameCommaEqualsCharactersEscaped() throws Exception {
        String originalUsername = "user,name=";
        String escapedUsername = "user=2Cname=3D";

        String expectedInitialResponseString = "n,,n=" + escapedUsername + ",r=" + CLIENT_NONCE;

        ScramSHA1Mechanism mech = new ScramSHA1Mechanism(CLIENT_NONCE);
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

        byte[] serverFirstMessage = "r=fyko+d2lbbFgONRv9qkxdawLdcbfa301-1618-46ee-96c1-2bf60139dc7f,s=Q0zM1qzKMOmI0sAzE7dXt6ru4ZIXhAzn40g4mQXKQdw=,i=4096".getBytes(StandardCharsets.UTF_8);
        byte[] expectedClientFinalMessage = "c=biws,r=fyko+d2lbbFgONRv9qkxdawLdcbfa301-1618-46ee-96c1-2bf60139dc7f,p=quRNWvZqGUvPXoazebZe0ZYsjQI=".getBytes(StandardCharsets.UTF_8);

        byte[] clientFinalMessage = mechanism.getChallengeResponse(serverFirstMessage);

        assertArrayEquals(expectedClientFinalMessage, clientFinalMessage);

        byte[] serverFinalMessage = "v=dnJDHm3fp6WwVrl5yjZuqKp03lQ=".getBytes(StandardCharsets.UTF_8);
        byte[] expectedFinalChallengeResponse = "".getBytes();

        assertArrayEquals(expectedFinalChallengeResponse, mechanism.getChallengeResponse(serverFinalMessage));

        mechanism.verifyCompletion();
    }

}