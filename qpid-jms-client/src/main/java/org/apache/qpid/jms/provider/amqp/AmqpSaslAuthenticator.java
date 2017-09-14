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

import java.util.function.Function;

import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;

import org.apache.qpid.jms.sasl.Mechanism;
import org.apache.qpid.proton.engine.Sasl;

/**
 * Manage the SASL authentication process
 */
public class AmqpSaslAuthenticator {

    private final Sasl sasl;
    private final Function<String[], Mechanism> mechanismFinder;

    private Mechanism mechanism;
    private boolean complete;
    private JMSSecurityException failureCause;

    /**
     * Create the authenticator and initialize it.
     *
     * @param sasl
     *        The Proton SASL entry point this class will use to manage the authentication.
     * @param mechanismFinder
     *        An object that is used to locate the most correct SASL Mechanism to perform the authentication.
     */
    public AmqpSaslAuthenticator(Sasl sasl, Function<String[], Mechanism> mechanismFinder) {
        this.sasl = sasl;
        this.mechanismFinder = mechanismFinder;
    }

    /**
     * Process the SASL authentication cycle until such time as an outcome is determine. This
     * method must be called by the managing entity until the return value is true indicating a
     * successful authentication or a JMSSecurityException is thrown indicating that the
     * handshake failed.
     */
    public void tryAuthenticate() {
        try {
            switch (sasl.getState()) {
                case PN_SASL_IDLE:
                    handleSaslInit();
                    break;
                case PN_SASL_STEP:
                    handleSaslStep();
                    break;
                case PN_SASL_FAIL:
                    handleSaslFail();
                    break;
                case PN_SASL_PASS:
                    handleSaslCompletion();
                    break;
                default:
                    break;
            }
        } catch (Throwable error) {
            recordFailure(error.getMessage(), error);
        }
    }

    public boolean isComplete() {
       return complete;
    }

    public JMSSecurityException getFailureCause() {
        return failureCause;
    }

    public boolean wasSuccessful() throws IllegalStateException {
        if (complete) {
            return failureCause == null;
        } else {
            throw new IllegalStateException("Authentication has not completed yet.");
        }
    }

    private void handleSaslInit() {
        try {
            String[] remoteMechanisms = sasl.getRemoteMechanisms();
            if (remoteMechanisms != null && remoteMechanisms.length != 0) {
                try {
                    mechanism = mechanismFinder.apply(remoteMechanisms);
                } catch (JMSSecurityRuntimeException jmssre){
                    recordFailure("Could not find a suitable SASL mechanism. " + jmssre.getMessage(), jmssre);
                    return;
                }

                byte[] response = mechanism.getInitialResponse();
                if (response != null) {
                    sasl.send(response, 0, response.length);
                }
                sasl.setMechanisms(mechanism.getName());
            }
        } catch (Throwable error) {
            recordFailure("Exception while processing SASL init: " + error.getMessage(), error);
        }
    }

    private void handleSaslStep() {
        try {
            if (sasl.pending() != 0) {
                byte[] challenge = new byte[sasl.pending()];
                sasl.recv(challenge, 0, challenge.length);
                byte[] response = mechanism.getChallengeResponse(challenge);
                if (response != null) {
                    sasl.send(response, 0, response.length);
                }
            }
        } catch (Throwable error) {
            recordFailure("Exception while processing SASL step: " + error.getMessage(), error);
        }
    }

    private void handleSaslFail() {
        if (mechanism != null) {
            recordFailure("Client failed to authenticate using SASL: " + mechanism.getName(), null);
        } else {
            recordFailure("Client failed to authenticate", null);
        }
    }

    private void handleSaslCompletion() {
        try {
            if (sasl.pending() != 0) {
                byte[] additionalData = new byte[sasl.pending()];
                sasl.recv(additionalData, 0, additionalData.length);
                mechanism.getChallengeResponse(additionalData);
            }
            mechanism.verifyCompletion();
            complete = true;
        } catch (Throwable error) {
            recordFailure("Exception while processing SASL exchange completion: " + error.getMessage(), error);
        }
    }

    private void recordFailure(String message, Throwable cause) {
        failureCause = new JMSSecurityException(message);
        if (cause instanceof Exception) {
            failureCause.setLinkedException((Exception) cause);
        }
        failureCause.initCause(cause);

        complete = true;
    }
}
