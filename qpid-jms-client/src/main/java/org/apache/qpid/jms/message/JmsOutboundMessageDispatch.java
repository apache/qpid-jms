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
    private Object dispatchId;

    public JmsDestination getDestination() {
        return destination;
    }

    public void setDestination(JmsDestination destination) {
        this.destination = destination;
    }

    public JmsMessage getMessage() {
        return message;
    }

    public void setMessage(JmsMessage message) {
        this.message = message;
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

    public Object getDispatchId() {
        return dispatchId;
    }

    public void setDispatchId(Object dispatchId) {
        this.dispatchId = dispatchId;
    }

    @Override
    public String toString() {
        String result = "JmsOutboundMessageDispatch {dispatchId = ";
        Object id = dispatchId;
        if (id == null) {
            result = result + "<null>}";
        } else {
            result = result + id + "}";
        }

        return result;
    }
}
