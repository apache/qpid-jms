/**
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

import javax.jms.JMSSecurityException;
import javax.security.sasl.SaslException;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.sasl.Mechanism;
import org.apache.qpid.jms.sasl.SaslMechanismFinder;
import org.apache.qpid.proton.engine.Sasl;

/**
 * Manage the SASL authentication process
 */
public class AmqpSaslAuthenticator {

    private final Sasl sasl;
    private final JmsConnectionInfo info;
    private Mechanism mechanism;

    /**
     * Create the authenticator and initialize it.
     *
     * @param sasl
     *        The Proton SASL entry point this class will use to manage the authentication.
     * @param info
     *        The Connection information used to provide credentials to the remote peer.
     */
    public AmqpSaslAuthenticator(Sasl sasl, JmsConnectionInfo info) {
        this.sasl = sasl;
        this.info = info;
    }

    /**
     * Process the SASL authentication cycle until such time as an outcome is determine. This
     * method must be called by the managing entity until the return value is true indicating a
     * successful authentication or a JMSSecurityException is thrown indicating that the
     * handshake failed.
     *
     * @throws JMSSecurityException
     */
    public boolean authenticate() throws JMSSecurityException {
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
                return true;
            default:
        }

        return false;
    }

    private void handleSaslInit() throws JMSSecurityException {
        try {
            String[] remoteMechanisms = sasl.getRemoteMechanisms();
            if (remoteMechanisms != null && remoteMechanisms.length != 0) {
                mechanism = SaslMechanismFinder.findMatchingMechanism(remoteMechanisms);
                if (mechanism != null) {
                    mechanism.setUsername(info.getUsername());
                    mechanism.setPassword(info.getPassword());
                    // TODO - set additional options from URI.
                    // TODO - set a host value.

                    sasl.setMechanisms(mechanism.getName());
                    byte[] response = mechanism.getInitialResponse();
                    if (response != null && response.length != 0) {
                        sasl.send(response, 0, response.length);
                    }
                } else {
                    // TODO - Better error message.
                    throw new JMSSecurityException("Could not find a matching SASL mechanism for the remote peer.");
                }
            }
        } catch (SaslException se) {
            // TODO - Better error message.
            JMSSecurityException jmsse = new JMSSecurityException("Exception while processing SASL init.");
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
            // TODO - Better error message.
            JMSSecurityException jmsse = new JMSSecurityException("Exception while processing SASL step.");
            jmsse.setLinkedException(se);
            jmsse.initCause(se);
            throw jmsse;
        }
    }

    private void handleSaslFail() throws JMSSecurityException {
        // TODO - Better error message.
        throw new JMSSecurityException("Client failed to authenticate");
    }
}
