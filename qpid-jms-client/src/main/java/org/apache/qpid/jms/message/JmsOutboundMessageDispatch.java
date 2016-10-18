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
package org.apache.qpid.jms.message;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.meta.JmsProducerId;

/**
 * Envelope that wraps the objects involved in a Message send operation.
 */
public class JmsOutboundMessageDispatch {

    private JmsProducerId producerId;
    private JmsMessage message;
    private JmsDestination destination;
    private boolean sendAsync;
    private boolean presettle;
    private boolean completionRequired;
    private long dispatchId;
    private Object payload;

    private transient String stringView;

    public JmsDestination getDestination() {
        return destination;
    }

    public void setDestination(JmsDestination destination) {
        this.destination = destination;
    }

    public Object getMessageId() {
        return message.getFacade().getProviderMessageIdObject();
    }

    public JmsMessage getMessage() {
        return message;
    }

    public void setMessage(JmsMessage message) {
        this.message = message;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public JmsProducerId getProducerId() {
        return producerId;
    }

    public void setProducerId(JmsProducerId producerId) {
        this.producerId = producerId;
    }

    public void setSendAsync(boolean sendAsync) {
        this.sendAsync = sendAsync;
    }

    public boolean isSendAsync() {
        return sendAsync;
    }

    public long getDispatchId() {
        return dispatchId;
    }

    public void setDispatchId(long dispatchId) {
        this.dispatchId = dispatchId;
    }

    public boolean isPresettle() {
        return presettle;
    }

    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    public boolean isCompletionRequired() {
        return completionRequired;
    }

    public void setCompletionRequired(boolean completionRequired) {
        this.completionRequired = completionRequired;
    }

    @Override
    public String toString() {
        if (stringView == null) {
            StringBuilder value = new StringBuilder();

            value.append("JmsOutboundMessageDispatch {dispatchId = ");
            value.append(getProducerId());
            value.append("-");
            value.append(getDispatchId());
            value.append(", MessageID = ");
            try {
                value.append(message.getJMSMessageID());
            } catch (Throwable e) {
                value.append("<unknown>");
            }
            value.append(" }");

            stringView = value.toString();
        }

        return stringView;
    }
}
