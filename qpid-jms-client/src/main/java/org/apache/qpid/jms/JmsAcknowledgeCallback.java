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

import static org.apache.qpid.jms.message.JmsMessageSupport.ACCEPTED;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsMessageSupport;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;

public final class JmsAcknowledgeCallback {

    private final JmsSession session;
    private int ackType;

    public JmsAcknowledgeCallback(JmsSession session) {
        this.session = session;
    }

    public void acknowledge() throws JMSException {
        if (session.isClosed()) {
            throw new javax.jms.IllegalStateException("Session closed.");
        }

        session.acknowledge(lookupAckTypeForDisposition(getAckType()));
    }

    private ACK_TYPE lookupAckTypeForDisposition(int dispositionType) throws JMSException {
        switch (dispositionType) {
            case JmsMessageSupport.ACCEPTED:
                return ACK_TYPE.ACCEPTED;
            case JmsMessageSupport.REJECTED:
                return ACK_TYPE.REJECTED;
            case JmsMessageSupport.RELEASED:
                return ACK_TYPE.RELEASED;
            case JmsMessageSupport.MODIFIED_FAILED:
                return ACK_TYPE.MODIFIED_FAILED;
            case JmsMessageSupport.MODIFIED_FAILED_UNDELIVERABLE:
                return ACK_TYPE.MODIFIED_FAILED_UNDELIVERABLE;
            default:
                throw new JMSException("Unable to determine ack type for disposition: " + dispositionType);
        }
    }

    /**
     * @return true if the acknowledgement type was updated.
     */
    public boolean isAckTypeSet() {
        return ackType > 0;
    }

    /**
     * Clears any previous setting and restores defaults.
     */
    public void clearAckType() {
        ackType = 0;
    }

    /**
     * @return the ackType that has been configured or the default if none has been set.
     */
    public int getAckType() {
        return ackType <= 0 ? ACCEPTED : ackType;
    }

    /**
     * Sets the acknowledgement type that will be used.
     *
     * @param ackType
     *      the ackType to apply to the session acknowledge.
     */
    public void setAckType(int ackType) {
        this.ackType = ackType;
    }
}
