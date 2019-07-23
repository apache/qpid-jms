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

import javax.jms.JMSSecurityException;

import org.apache.qpid.jms.provider.ProviderException;

/**
 * Security Exception used to indicate a security violation has occurred that is non-fatal
 * such as link creation blocked because user does not have access etc.
 */
public class ProviderSecurityException extends ProviderException {

    private static final long serialVersionUID = -1895132556606592253L;

    public ProviderSecurityException(String message) {
        super(message);
    }

    public ProviderSecurityException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public JMSSecurityException toJMSException() {
        JMSSecurityException jmsEx = new JMSSecurityException(getMessage());
        jmsEx.initCause(this);
        jmsEx.setLinkedException(this);
        return jmsEx;
    }
}
