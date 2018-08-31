/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/
package org.apache.qpid.jms.exceptions;

import javax.jms.JMSSecurityException;

public class JMSSecuritySaslException extends JMSSecurityException {

    private static final long serialVersionUID = -6181892492517836496L;
    private static final int SASL_SYS_TEMP = 4;

    private int outcome = -1;

    public JMSSecuritySaslException(String reason) {
        super(reason);
    }

    public JMSSecuritySaslException(String reason, int outcome) {
        super(reason);
        this.outcome = outcome;
    }

    public boolean isSysTempFailure() {
        return outcome == SASL_SYS_TEMP;
    }
}