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
package org.apache.qpid.jms.provider.amqp;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;
import java.util.function.Function;

import javax.jms.JMSSecurityRuntimeException;
import javax.security.sasl.SaslException;

import org.apache.qpid.jms.sasl.Mechanism;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Sasl.SaslState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class AmqpSaslAuthenticatorTest {

    private static final String MECHANISM_NAME = "MY_MECHANISM";
    private static final byte[] INITIAL_RESPONSE = "initial".getBytes();
    private static final byte[] RESPONSE = "response".getBytes();
    private static final byte[] EXPECTED_CHALLENGE = "expectedChallenge".getBytes();
    private static final byte[] EMPTY_BYTES = {};

    private final Sasl sasl = mock(Sasl.class);

    @Before
    public void setUp() {
        when(sasl.getState()).thenReturn(SaslState.PN_SASL_IDLE);
        when(sasl.getRemoteMechanisms()).thenReturn(new String[]{MECHANISM_NAME});
    }

    @Test
    public void testNullSaslMechanismReturnedErroneously() throws Exception {
        Function<String[], Mechanism> mechanismFunction = mechanismName -> null;

        AmqpSaslAuthenticator authenticator = new AmqpSaslAuthenticator(sasl, mechanismFunction);
        authenticator.tryAuthenticate();

        assertTrue(authenticator.isComplete());
        assertFalse(authenticator.wasSuccessful());
        assertNotNull(authenticator.getFailureCause());
        assertTrue(authenticator.getFailureCause().getMessage().startsWith("Exception while processing SASL init:"));
    }

    @Test
    public void testNoSaslMechanismAgreed() throws Exception {
        Function<String[], Mechanism> mechanismFunction = mechanismName -> {
            throw new JMSSecurityRuntimeException("reasons");
        };

        AmqpSaslAuthenticator authenticator = new AmqpSaslAuthenticator(sasl, mechanismFunction);
        authenticator.tryAuthenticate();

        assertTrue(authenticator.isComplete());
        assertFalse(authenticator.wasSuccessful());
        assertNotNull(authenticator.getFailureCause());
        assertTrue(authenticator.getFailureCause().getMessage().contains("Could not find a suitable SASL mechanism"));
    }

    @Test
    public void testAuthenticationSuccess() throws Exception {
        Mechanism mechanism = new TestSaslMechanism(INITIAL_RESPONSE, EXPECTED_CHALLENGE, RESPONSE);
        AmqpSaslAuthenticator authenticator = new AmqpSaslAuthenticator(sasl, mechanismName -> mechanism);

        authenticator.tryAuthenticate();
        verify(sasl).setMechanisms(mechanism.getName());
        verifySaslMockReceived(sasl, INITIAL_RESPONSE);

        when(sasl.getState()).thenReturn(SaslState.PN_SASL_STEP);
        configureSaslMockToProduce(sasl, EXPECTED_CHALLENGE);
        authenticator.tryAuthenticate();
        verifySaslMockReceived(sasl, RESPONSE);

        when(sasl.getState()).thenReturn(SaslState.PN_SASL_PASS);
        configureSaslMockToProduce(sasl, EMPTY_BYTES);
        authenticator.tryAuthenticate();

        assertTrue(authenticator.isComplete());
        assertTrue(authenticator.wasSuccessful());
        assertNull(authenticator.getFailureCause());
    }

    @Test
    public void testAuthenticationSuccessWithoutChallengeStep() throws Exception {
        Mechanism mechanism = new TestSaslMechanism(INITIAL_RESPONSE);
        AmqpSaslAuthenticator authenticator = new AmqpSaslAuthenticator(sasl, mechanismName -> mechanism);

        authenticator.tryAuthenticate();
        verifySaslMockReceived(sasl, INITIAL_RESPONSE);

        when(sasl.getState()).thenReturn(SaslState.PN_SASL_PASS);
        configureSaslMockToProduce(sasl, EMPTY_BYTES);
        authenticator.tryAuthenticate();

        assertTrue(authenticator.isComplete());
        assertTrue(authenticator.wasSuccessful());
    }

    @Test
    public void testPeerSignalsAuthenticationFail() throws Exception {
        Mechanism mechanism = new TestSaslMechanism(INITIAL_RESPONSE);
        AmqpSaslAuthenticator authenticator = new AmqpSaslAuthenticator(sasl, mechanismName -> mechanism);

        authenticator.tryAuthenticate();
        verifySaslMockReceived(sasl, INITIAL_RESPONSE);

        when(sasl.getState()).thenReturn(SaslState.PN_SASL_FAIL);
        authenticator.tryAuthenticate();

        assertTrue(authenticator.isComplete());
        assertFalse(authenticator.wasSuccessful());
        assertNotNull(authenticator.getFailureCause());
        assertTrue(authenticator.getFailureCause().getMessage().contains("Client failed to authenticate"));
    }

    @Test
    public void testMechanismSignalsNotComplete() throws Exception {
        Mechanism mechanism = new TestSaslMechanism(INITIAL_RESPONSE,
                                                    EXPECTED_CHALLENGE, RESPONSE,
                                                    EMPTY_BYTES, EMPTY_BYTES);
        AmqpSaslAuthenticator authenticator = new AmqpSaslAuthenticator(sasl, mechanismName -> mechanism);

        when(sasl.getState()).thenReturn(SaslState.PN_SASL_IDLE);
        authenticator.tryAuthenticate();
        verifySaslMockReceived(sasl, INITIAL_RESPONSE);

        when(sasl.getState()).thenReturn(SaslState.PN_SASL_STEP);
        configureSaslMockToProduce(sasl, EXPECTED_CHALLENGE);
        authenticator.tryAuthenticate();
        verifySaslMockReceived(sasl, RESPONSE);

        when(sasl.getState()).thenReturn(SaslState.PN_SASL_PASS);
        configureSaslMockToProduce(sasl, EMPTY_BYTES);
        authenticator.tryAuthenticate();

        assertTrue(authenticator.isComplete());
        assertFalse(authenticator.wasSuccessful());
        assertNotNull(authenticator.getFailureCause());
        assertTrue(authenticator.getFailureCause().getMessage().contains("SASL exchange not completed"));
    }

    private void verifySaslMockReceived(final Sasl sasl, final byte[] response) {
        verify(sasl).send(response, 0, response.length);
    }

    private void configureSaslMockToProduce(final Sasl sasl, final byte[] challenge) {
        when(sasl.pending()).thenReturn(challenge.length);
        Answer<Void> answer = invocationOnMock -> {
            byte[] buf = invocationOnMock.getArgument(0);
            int offset = invocationOnMock.getArgument(1);
            int length = invocationOnMock.getArgument(2);
            System.arraycopy(challenge, 0, buf, offset, Math.min(length, challenge.length));
            return null;
        };
        doAnswer(answer).when(sasl).recv(any(byte[].class), any(int.class), any(int.class));
    }

    private class TestSaslMechanism implements Mechanism {

        private final byte[] initialResponse;
        private final Deque<byte[]> challengeExpectationResponseSequence;

        TestSaslMechanism(final byte[] initialResponse, final byte[]... challengeExpectationResponseSequence) {
            if (challengeExpectationResponseSequence.length % 2 != 0) {
                throw new IllegalStateException("Number of items in the challenge expectation / response array must be even");
            }
            this.initialResponse = initialResponse;
            this.challengeExpectationResponseSequence = new ArrayDeque<>(Arrays.asList(challengeExpectationResponseSequence));
        }

        @Override
        public int getPriority() {
            return 0;
        }

        @Override
        public String getName() {
            return MECHANISM_NAME;
        }

        @Override
        public void init(final Map<String, String> options) {
        }

        @Override
        public byte[] getInitialResponse() {
            return initialResponse;
        }

        @Override
        public byte[] getChallengeResponse(final byte[] challenge) throws SaslException {
            final byte[] expectedChallenge = challengeExpectationResponseSequence.removeFirst();
            final byte[] response = challengeExpectationResponseSequence.removeFirst();

            if (!Arrays.equals(expectedChallenge, challenge)) {
                throw new SaslException("Challenge does not meet the challenge expectation");
            }
            return response;
        }

        @Override
        public void verifyCompletion() throws SaslException {
            if (!challengeExpectationResponseSequence.isEmpty()) {
                throw new SaslException("SASL exchange not completed");
            }
        }

        @Override
        public void setUsername(final String username) {
        }

        @Override
        public String getUsername() {
            return null;
        }

        @Override
        public void setPassword(final String username) {
        }

        @Override
        public String getPassword() {
            return null;
        }

        @Override
        public boolean isApplicable(final String username, final String password, final Principal localPrincipal) {
            return true;
        }

        @Override
        public int compareTo(final Mechanism o) {
            return 0;
        }

        @Override
        public boolean isEnabledByDefault() {
            return true;
        }
    }
}
