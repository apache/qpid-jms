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
import javax.security.sasl.SaslException;

import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.sasl.Mechanism;
import org.apache.qpid.proton.engine.Sasl;

/**
 * Manage the SASL authentication process
 */
public class AmqpSaslAuthenticator {

    private final Sasl sasl;
    private final AsyncResult authenticationRequest;
    private final Function<String[], Mechanism> mechanismFinder;

    private Mechanism mechanism;
    private boolean complete;

    /**
     * Create the authenticator and initialize it.
     *
     * @param request
     * 	      The initial request that is awaiting the result of the authentication process.
     * @param sasl
     *        The Proton SASL entry point this class will use to manage the authentication.
     * @param mechanismFinder
     *        An object that is used to locate the most correct SASL Mechanism to perform the authentication.
     */
    public AmqpSaslAuthenticator(AsyncResult request, Sasl sasl, Function<String[], Mechanism> mechanismFinder) {
        this.sasl = sasl;
        this.authenticationRequest = request;
        this.mechanismFinder = mechanismFinder;
    }

    /**
     * Process the SASL authentication cycle until such time as an outcome is determine. This
     * method must be called by the managing entity until the return value is true indicating a
     * successful authentication or a JMSSecurityException is thrown indicating that the
     * handshake failed.
     *
     * @return true if the authentication process completed.
     */
    public boolean authenticate() {
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
                    authenticationRequest.onSuccess();
                default:
                    break;
            }
        } catch (JMSSecurityException result) {
            authenticationRequest.onFailure(result);
        }

        complete = authenticationRequest.isComplete();

        return complete;
    }

    public boolean isComplete() {
       return complete;
    }

    public boolean wasSuccessful() throws IllegalStateException {
        switch (sasl.getState()) {
            case PN_SASL_CONF:
            case PN_SASL_IDLE:
            case PN_SASL_STEP:
                break;
            case PN_SASL_FAIL:
                return false;
            case PN_SASL_PASS:
                return true;
            default:
                break;
        }

        throw new IllegalStateException("Authentication has not completed yet.");
    }

    private void handleSaslInit() throws JMSSecurityException {
        try {
            String[] remoteMechanisms = sasl.getRemoteMechanisms();
            if (remoteMechanisms != null && remoteMechanisms.length != 0) {
                mechanism = mechanismFinder.apply(remoteMechanisms);
                if (mechanism != null) {
                    sasl.setMechanisms(mechanism.getName());
                    byte[] response = mechanism.getInitialResponse();
                    if (response != null) {
                        sasl.send(response, 0, response.length);
                    }
                } else {
                    throw new JMSSecurityException("Could not find a suitable SASL mechanism for the remote peer using the available credentials.");
                }
            }
        } catch (SaslException se) {
            JMSSecurityException jmsse = new JMSSecurityException("Exception while processing SASL init: " + se.getMessage());
            jmsse.setLinkedException(se);
            jmsse.initCause(se);
            throw jmsse;
        }
    }

    private void handleSaslStep() throws JMSSecurityException {
        try {
            if (sasl.pending() != 0) {
                byte[] challenge = new byte[sasl.pending()];
                sasl.recv(challenge, 0, challenge.length);
                byte[] response = mechanism.getChallengeResponse(challenge);
                sasl.send(response, 0, response.length);
            }
        } catch (SaslException se) {
            JMSSecurityException jmsse = new JMSSecurityException("Exception while processing SASL step: " + se.getMessage());
            jmsse.setLinkedException(se);
            jmsse.initCause(se);
            throw jmsse;
        }
    }

    private void handleSaslFail() throws JMSSecurityException {
        if (mechanism != null) {
            throw new JMSSecurityException("Client failed to authenticate using SASL: " + mechanism.getName());
        } else {
            throw new JMSSecurityException("Client failed to authenticate");
        }
    }
}
