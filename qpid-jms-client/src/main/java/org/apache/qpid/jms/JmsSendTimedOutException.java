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
package org.apache.qpid.jms;

import javax.jms.Message;

import org.apache.qpid.jms.message.JmsMessage;

/**
 * Exception thrown when a blocking send call times out waiting to be sent.
 */
public class JmsSendTimedOutException extends JmsOperationTimedOutException {

    private static final long serialVersionUID = -5404637534865352615L;

    private final JmsMessage unsentMessage;

    public JmsSendTimedOutException(String reason) {
        this(reason, null, null);
    }

    public JmsSendTimedOutException(String reason, String errorCode) {
        this(reason, errorCode, null);
    }

    public JmsSendTimedOutException(String reason, JmsMessage unsentMessage) {
        this(reason, null, unsentMessage);
    }

    public JmsSendTimedOutException(String reason, String errorCode, JmsMessage unsentMessage) {
        super(reason, errorCode);
        this.unsentMessage = unsentMessage;
    }

    public Message getUnsentMessage() {
        return unsentMessage;
    }
}
