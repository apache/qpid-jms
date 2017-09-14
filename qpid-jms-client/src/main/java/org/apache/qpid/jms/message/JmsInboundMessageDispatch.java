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

import org.apache.qpid.jms.meta.JmsAbstractResourceId;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;

/**
 * Envelope used to deliver incoming messages to their targeted consumer.
 */
public class JmsInboundMessageDispatch extends JmsAbstractResourceId {

    private JmsConsumerId consumerId;
    private Object messageId;
    private final long sequence;
    private JmsMessage message;
    private boolean enqueueFirst;
    private boolean delivered;

    private transient JmsConsumerInfo consumerInfo;
    private transient String stringView;

    public JmsInboundMessageDispatch(long sequence) {
        this.sequence = sequence;
    }

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

    public void setMessageId(Object object) {
        this.messageId = object;
    }

    public void setEnqueueFirst(boolean insertAtFront) {
        this.enqueueFirst = insertAtFront;
    }

    public boolean isEnqueueFirst() {
        return enqueueFirst;
    }

    public boolean isDelivered() {
        return delivered;
    }

    public void setDelivered(boolean delivered) {
        this.delivered = delivered;
    }

    public int getRedeliveryCount() {
        int redeliveryCount = 0;

        if (message != null) {
            redeliveryCount = message.getFacade().getRedeliveryCount();
        }

        return redeliveryCount;
    }

    public JmsConsumerInfo getConsumerInfo() {
        return consumerInfo;
    }

    public void setConsumerInfo(JmsConsumerInfo consumerInfo) {
        this.consumerInfo = consumerInfo;
    }

    @Override
    public String toString() {
        if (stringView == null) {
            StringBuilder builder = new StringBuilder();

            builder.append("JmsInboundMessageDispatch { sequence = ");
            builder.append(sequence);
            builder.append(", messageId = ");
            builder.append(messageId);
            builder.append(", consumerId = ");
            builder.append(consumerId);
            builder.append(" }");

            stringView = builder.toString();
        }

        return stringView;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerId == null) ? 0 : consumerId.hashCode());
        result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
        result = prime * result + (int) (sequence ^ (sequence >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        JmsInboundMessageDispatch other = (JmsInboundMessageDispatch) obj;
        if (sequence != other.sequence) {
            return false;
        }

        if (messageId == null) {
            if (other.messageId != null) {
                return false;
            }
        } else if (!messageId.equals(other.messageId)) {
            return false;
        }

        if (consumerId == null) {
            if (other.consumerId != null) {
                return false;
            }
        } else if (!consumerId.equals(other.consumerId)) {
            return false;
        }

        return true;
    }
}
