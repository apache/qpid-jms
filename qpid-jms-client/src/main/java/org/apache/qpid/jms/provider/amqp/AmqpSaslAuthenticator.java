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

import org.apache.qpid.jms.sasl.Mechanism;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;

import java.util.function.Function;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;

/**
 * Manage the SASL authentication process
 */
public class AmqpSaslAuthenticator {

    private final Function<String[], Mechanism> mechanismFinder;

    private Mechanism mechanism;
    private boolean complete;
    private JMSSecurityException failureCause;

    /**
     * Create the authenticator and initialize it.
     *
     * @param mechanismFinder
     *        An object that is used to locate the most correct SASL Mechanism to perform the authentication.
     */
    public AmqpSaslAuthenticator(Function<String[], Mechanism> mechanismFinder) {
        this.mechanismFinder = mechanismFinder;
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

    //----- SaslListener implementation --------------------------------------//

    public void handleSaslMechanisms(Sasl sasl, Transport transport) {
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

    public void handleSaslChallenge(Sasl sasl, Transport transport) {
        try {
            if (sasl.pending() >= 0) {
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

    public void handleSaslOutcome(Sasl sasl, Transport transport) {
        try {
            switch (sasl.getState()) {
                case PN_SASL_FAIL:
                    handleSaslFail();
                    break;
                case PN_SASL_PASS:
                    handleSaslCompletion(sasl);
                    break;
                default:
                    break;
            }
        } catch (Throwable error) {
            recordFailure(error.getMessage(), error);
        }
    }

    //----- Internal support methods -----------------------------------------//

    private void handleSaslFail() {
        StringBuilder message = new StringBuilder("Client failed to authenticate");
        if (mechanism != null) {
            message.append(" using SASL: ").append(mechanism.getName());
            if (mechanism.getAdditionalFailureInformation() != null) {
                message.append(" (").append(mechanism.getAdditionalFailureInformation()).append(")");
            }
        }
        recordFailure(message.toString(), null);
    }

    private void handleSaslCompletion(Sasl sasl) {
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
