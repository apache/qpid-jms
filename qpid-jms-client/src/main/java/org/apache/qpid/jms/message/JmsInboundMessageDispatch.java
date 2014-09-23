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

import org.apache.qpid.jms.meta.JmsConsumerId;

/**
 * Envelope used to deliver incoming messages to their targeted consumer.
 */
public class JmsInboundMessageDispatch {

    private JmsConsumerId consumerId;
    private JmsMessage message;
    private Object providerHint;

    public JmsMessage getMessage() {
        return message;
    }

    public void setMessage(JmsMessage message) {
        this.message = message;
    }

    public JmsConsumerId getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(JmsConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    public Object getProviderHint() {
        return this.providerHint;
    }

    public void setProviderHint(Object hint) {
        this.providerHint = hint;
    }

    public void onMessageRedelivered() {
        this.message.incrementRedeliveryCount();
    }
}
