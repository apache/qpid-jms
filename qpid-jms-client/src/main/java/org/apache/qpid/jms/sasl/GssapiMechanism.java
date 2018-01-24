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

import org.apache.qpid.jms.util.PropertyUtil;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements the GSSAPI sasl authentication Mechanism.
 */
public class GssapiMechanism extends AbstractMechanism {

    public static final String NAME = "GSSAPI";

    private Subject subject;
    private SaslClient saslClient;
    private String protocol = "amqp";
    private String serverName = null;
    private String configScope = "amqp-jms-client";

    // a gss/sasl service name, x@y, morphs to a krbPrincipal a/y@REALM

    @Override
    public int getPriority() {
        return PRIORITY.LOW.getValue();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isEnabledByDefault() {
        // Only enable if given explicit configuration to do so, as we can't discern here
        // whether the external configuration is appropriately set to actually allow its use.
        return false;
    }

    @Override
    public void init(Map<String, String> saslOptions) {
        PropertyUtil.setProperties(this, saslOptions);
    }

    @Override
    public byte[] getInitialResponse() throws SaslException {
        try {
            LoginContext loginContext = new LoginContext(configScope, new CredentialCallbackHandler());;

            loginContext.login();
            subject = loginContext.getSubject();

            return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {

                @Override
                public byte[] run() throws Exception {
                    Map<String, String> props = new HashMap<>();
                    props.put("javax.security.sasl.server.authentication", "true");

                    saslClient = Sasl.createSaslClient(new String[]{NAME}, null, protocol, serverName, props, null);
                    if (saslClient.hasInitialResponse()) {
                        return saslClient.evaluateChallenge(new byte[0]);
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            throw new SaslException(e.toString(), e);
        }
    }

    @Override
    public byte[] getChallengeResponse(final byte[] challenge) throws SaslException {
        try {
            return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                @Override
                public byte[] run() throws Exception {
                    return saslClient.evaluateChallenge(challenge);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new SaslException(e.toString(), e);
        }
    }

    @Override
    public void verifyCompletion() throws SaslException {
        boolean result = saslClient.isComplete();
        saslClient.dispose();
        if (!result) {
            throw new SaslException("not complete");
        }
    }

    @Override
    public boolean isApplicable(String username, String password, Principal localPrincipal) {
        return true;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getConfigScope() {
        return configScope;
    }

    public void setConfigScope(String configScope) {
        this.configScope = configScope;
    }

    private class CredentialCallbackHandler implements CallbackHandler {

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
                Callback cb = callbacks[i];
                if (cb instanceof NameCallback) {
                    ((NameCallback) cb).setName(getUsername());
                } else if (cb instanceof PasswordCallback) {
                    String pass = getPassword();
                    if (pass != null) {
                        ((PasswordCallback) cb).setPassword(pass.toCharArray());
                    }
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }
}
