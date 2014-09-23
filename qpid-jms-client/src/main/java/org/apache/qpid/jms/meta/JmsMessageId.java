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


/**
 * JMS Message Id class used to uniquely identify messages for the JMS Framework.
 */
public class JmsMessageId extends JmsAbstractResourceId implements Comparable<JmsMessageId> {

    protected Object messageId;

    public JmsMessageId(JmsProducerInfo producerInfo, long producerSequenceId) {
        this(producerInfo.getProducerId(), producerSequenceId);
    }

    public JmsMessageId(JmsProducerId producerId, long producerSequenceId) {
        this(producerId.toString(), producerSequenceId);
    }

    public JmsMessageId(String producerId, long producerSequenceId) {
        this(producerId + "-" + producerSequenceId);
    }

    public JmsMessageId(Object messageId) {
        setValue(messageId);
    }

    public JmsMessageId copy() {
        JmsMessageId copy = new JmsMessageId(messageId);
        return copy;
    }

    /**
     * Sets the value as a opaque object
     *
     * @param messageId
     *        The new message Id value for this instance.
     */
    public void setValue(Object messageId) {
        this.messageId = messageId;
    }

    /**
     * @return the set message ID value.
     */
    public Object getValue() {
        return messageId;
    }

    @Override
    public boolean equals(Object o) {
        //TODO: handle messages with no messageId value
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        JmsMessageId id = (JmsMessageId) o;
        return id.messageId.equals(this.messageId);
    }

    @Override
    public int hashCode() {
        //TODO: handle messages with no messageId value
        if (hashCode == 0) {
            hashCode = messageId.hashCode();
        }
        return hashCode;
    }

    @Override
    public int compareTo(JmsMessageId other) {
        //TODO: handle messages with no messageId value
        int result = -1;
        if (other != null) {
            result = this.toString().compareTo(other.toString());
        }
        return result;
    }

    @Override
    public String toString() {
        String result = messageId.toString();
        if (result != null) {
            if (!result.startsWith("ID:")) {
                result = "ID:" + messageId;
            }
        }

        return result;
    }
}
