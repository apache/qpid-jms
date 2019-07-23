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
package org.apache.qpid.jms.provider.exceptions;

import org.apache.qpid.jms.exceptions.JMSSecuritySaslException;

/**
 * Security Exception used to indicate a security violation has occurred.
 */
public class ProviderConnectionSecuritySaslException extends ProviderConnectionSecurityException {

    private static final long serialVersionUID = 313318720407251822L;
    private static final int SASL_SYS_TEMP = 4;

    private int outcome = -1;

    public ProviderConnectionSecuritySaslException(String message) {
        this(message, -1, null);
    }

    public ProviderConnectionSecuritySaslException(String message, int outcome) {
        this(message, outcome, null);
    }

    public ProviderConnectionSecuritySaslException(String message, int outcome, Throwable cause) {
        super(message, cause);

        this.outcome = outcome;
    }

    public boolean isSysTempFailure() {
        return outcome == SASL_SYS_TEMP;
    }

    @Override
    public JMSSecuritySaslException toJMSException() {
        JMSSecuritySaslException jmsEx = new JMSSecuritySaslException(getMessage(), outcome);
        jmsEx.initCause(this);
        jmsEx.setLinkedException(this);
        return jmsEx;
    }
}
