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

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
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
    private String configScope = null;

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
    public byte[] getInitialResponse() throws SaslException {
        try {
            LoginContext loginContext = null;
            if (configScope != null) {
                loginContext = new LoginContext(configScope);
            } else {
                // inline keytab config using user as principal
                loginContext = new LoginContext("", null, null,
                        kerb5InlineConfig(getUsername(), true));
            }
            loginContext.login();
            subject = loginContext.getSubject();

            return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {

                @Override
                public byte[] run() throws Exception {
                    saslClient = Sasl.createSaslClient(new String[]{NAME}, null, protocol, serverName, null, null);
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

    private static final boolean IBM_JAVA =  System.getProperty("java.vendor").contains("IBM");
    public static Configuration kerb5InlineConfig(String principal, boolean initiator) {
        final Map<String, String> options = new HashMap<>();
        if (IBM_JAVA) {
            options.put("principal", principal);
            options.put("renewable", "true");
            options.put("credsType", initiator ? "initiator" : "acceptor");
        } else {
            options.put("isInitiator", String.valueOf(initiator));
            options.put("principal", principal);
            options.put("useKeyTab", "true");
            options.put("storeKey", "true");
            options.put("doNotPrompt", "true");
            options.put("renewTGT", "true");
            options.put("refreshKrb5Config", "true");
            options.put("useTicketCache", "true");
            String ticketCache = System.getenv("KRB5CCNAME");

            if (IBM_JAVA) {
                // If cache is specified via env variable, it takes priority
                if (ticketCache != null) {
                    // IBM JAVA only respects system property so copy ticket cache to system property
                    // The first value searched when "useDefaultCcache" is true.
                    System.setProperty("KRB5CCNAME", ticketCache);
                } else {
                    ticketCache = System.getProperty("KRB5CCNAME");
                }
                if (ticketCache != null) {
                    options.put("useDefaultCcache", "true");
                }
            } else {
                if (ticketCache != null) {
                    options.put("ticketCache", ticketCache);
                }
            }
        }
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(getKrb5LoginModuleName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)};
            }
        };
    }

    private static String getKrb5LoginModuleName() {
        return IBM_JAVA ? "com.ibm.security.auth.module.Krb5LoginModule"
                : "com.sun.security.auth.module.Krb5LoginModule";
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
}
