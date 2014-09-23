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
package org.apache.qpid.jms.meta;

import javax.jms.Session;

public final class JmsSessionInfo implements JmsResource {

    private final JmsSessionId sessionId;
    private int acknowledgementMode;
    private boolean sendAcksAsync;

    public JmsSessionInfo(JmsConnectionInfo connectionMeta, long sessionId) {
        this.sessionId = new JmsSessionId(connectionMeta.getConnectionId(), sessionId);
    }

    public JmsSessionInfo(JmsSessionId sessionId) {
        this.sessionId = sessionId;
    }

    public JmsSessionId getSessionId() {
        return sessionId;
    }

    @Override
    public void visit(JmsResourceVistor vistor) throws Exception {
        vistor.processSessionInfo(this);
    }

    public int getAcknowledgementMode() {
        return acknowledgementMode;
    }

    public void setAcknowledgementMode(int acknowledgementMode) {
        this.acknowledgementMode = acknowledgementMode;
    }

    public boolean isTransacted() {
        return this.acknowledgementMode == Session.SESSION_TRANSACTED;
    }

    public boolean isSendAcksAsync() {
        return sendAcksAsync;
    }

    public void setSendAcksAsync(boolean sendAcksAsync) {
        this.sendAcksAsync = sendAcksAsync;
    }
}
