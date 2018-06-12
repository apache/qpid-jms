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

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsResourceId;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderSynchronization;

/**
 * Used in non-transacted JMS Sessions to throw proper errors indicating
 * that the Session is not transacted and cannot be treated as such.
 */
public class JmsNoTxTransactionContext implements JmsTransactionContext {

    @Override
    public void send(JmsConnection connection, JmsOutboundMessageDispatch envelope, ProviderSynchronization outcome) throws JMSException {
        connection.send(envelope, outcome);
    }

    @Override
    public void acknowledge(JmsConnection connection, JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        connection.acknowledge(envelope, ackType);
    }

    @Override
    public boolean isInDoubt() {
        return false;
    }

    @Override
    public void begin() throws JMSException {
    }

    @Override
    public void rollback() throws JMSException {
    }

    @Override
    public void shutdown() throws JMSException {
    }

    @Override
    public void commit() throws JMSException {
    }

    @Override
    public JmsTransactionId getTransactionId() {
        return null;
    }

    @Override
    public JmsTransactionListener getListener() {
        return null;
    }

    @Override
    public void setListener(JmsTransactionListener listener) {
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }

    @Override
    public boolean isActiveInThisContext(JmsResourceId resouceId) {
        return false;
    }

    @Override
    public void onConnectionInterrupted() {
    }

    @Override
    public void onConnectionRecovery(Provider provider) throws Exception {
    }
}
