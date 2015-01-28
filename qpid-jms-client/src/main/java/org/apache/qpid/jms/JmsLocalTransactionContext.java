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
package org.apache.qpid.jms;

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the details of a Session operating inside of a local JMS transaction.
 */
public class JmsLocalTransactionContext implements JmsTransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(JmsLocalTransactionContext.class);

    private List<JmsTxSynchronization> synchronizations;
    private final JmsSession session;
    private final JmsConnection connection;
    private JmsTransactionId transactionId;
    private boolean failed;
    private JmsTransactionListener listener;

    public JmsLocalTransactionContext(JmsSession session) {
        this.session = session;
        this.connection = session.getConnection();
    }

    @Override
    public void send(JmsConnection connection, JmsOutboundMessageDispatch envelope) throws JMSException {
        // TODO - Optional throw an exception here to give early warning that
        //        the transaction is in a failed state and must be rolled back.

        //TODO: Is it worth holding the producer here (or earlier) while recovery is known to be in progress?
        if (!isFailed()) {
            begin();
            connection.send(envelope);
        }
    }

    @Override
    public void acknowledge(JmsConnection connection, JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        // TODO - Should we ACK as delivered if failed just to ensure that prefetch is
        //        extended and new message arrive until commit or rollback is called.
        //        A quiet consumer could be misleading and prevent the code from doing
        //        its normal batched receive / commit.
        //
        //        Reply: I think at least we would want a way to replenish the credit,
        //        even if we didn't call the ack method (avoiding using the provider or
        //        pumping the proton transport), especially if it was a low-prefetch
        //        consumer to begin with. I think we always 'consumed ack' transacted
        //        messages currently, never 'delivered ack' since we don't need to do
        //        session recover for them.
        if (!isFailed()) {
            // Consumed or delivered messages fall into a transaction so we must check
            // that there is an active one and start one if not.
            if (ackType == ACK_TYPE.CONSUMED || ackType == ACK_TYPE.DELIVERED) {
                begin();
            }

            connection.acknowledge(envelope, ackType);
        }
    }

    @Override
    public void addSynchronization(JmsTxSynchronization s) {
        if (synchronizations == null) {
            synchronizations = new ArrayList<JmsTxSynchronization>(10);
        }
        synchronizations.add(s);
    }

    @Override
    public void markAsFailed() {
        //TODO: do we need to adjust this (or perhaps when we start the transaction?)
        //      to handle an ack for a stale message delivery via onMessage starting
        //      a transaction after this method was originally called?
        if (isInTransaction()) {
            failed = true;
        }
    }

    @Override
    public boolean isFailed() {
        return failed;
    }

    @Override
    public void begin() throws JMSException {
        if (!isInTransaction()) {
            synchronizations = null;
            failed = false;

            transactionId = connection.getNextTransactionId();
            JmsTransactionInfo transaction = new JmsTransactionInfo(session.getSessionId(), transactionId);
            connection.createResource(transaction);

            if (listener != null) {
                listener.onTransactionStarted();
            }

            LOG.debug("Begin: {}", transactionId);
        }
    }

    @Override
    public void rollback() throws JMSException {

        if (isFailed()) {
            LOG.debug("Rollback of already failed TX: {} syncCount: {}", transactionId,
                      (synchronizations != null ? synchronizations.size() : 0));

            failed = false;
            transactionId = null;
        }

        if (isInTransaction()) {
            LOG.debug("Rollback: {} syncCount: {}", transactionId,
                      (synchronizations != null ? synchronizations.size() : 0));

            failed = false;
            transactionId = null;
            connection.rollback(session.getSessionId());

            if (listener != null) {
                listener.onTransactionRolledBack();
            }
        }

        afterRollback();
    }

    @Override
    public void commit() throws JMSException {
        if (isFailed()) {
            failed = false;
            transactionId = null;
            //TODO: we need to actually roll back if we have let any acks etc occur after the recovery.
            throw new TransactionRolledBackException("Transaction failed and has been rolled back.");
        }

        if (isInTransaction()) {
            LOG.debug("Commit: {} syncCount: {}", transactionId,
                      (synchronizations != null ? synchronizations.size() : 0));

            JmsTransactionId oldTransactionId = this.transactionId;
            transactionId = null;
            try {
                connection.commit(session.getSessionId());
                if (listener != null) {
                    listener.onTransactionCommitted();
                }
                afterCommit();
            } catch (JMSException cause) {
                LOG.info("Commit failed for transaction: {}", oldTransactionId);
                if (listener != null) {
                    listener.onTransactionRolledBack();
                }
                afterRollback();
                throw cause;
            }
        }
    }

    @Override
    public String toString() {
        return "JmsLocalTransactionContext{ transactionId=" + transactionId + " }";
    }

    //------------- Getters and Setters --------------------------------------//

    @Override
    public JmsTransactionId getTransactionId() {
        return transactionId;
    }

    @Override
    public JmsTransactionListener getListener() {
        return listener;
    }

    @Override
    public void setListener(JmsTransactionListener listener) {
        this.listener = listener;
    }

    @Override
    public boolean isInTransaction() {
        return transactionId != null;
    }

    //------------- Implementation methods -----------------------------------//

    private void afterRollback() throws JMSException {
        if (synchronizations == null) {
            return;
        }

        Throwable firstException = null;
        int size = synchronizations.size();
        for (int i = 0; i < size; i++) {
            try {
                synchronizations.get(i).afterRollback();
            } catch (Throwable thrown) {
                LOG.debug("Exception from afterRollback on " + synchronizations.get(i), thrown);
                if (firstException == null) {
                    firstException = thrown;
                }
            }
        }
        synchronizations = null;
        if (firstException != null) {
            throw JmsExceptionSupport.create(firstException);
        }
    }

    private void afterCommit() throws JMSException {
        if (synchronizations == null) {
            return;
        }

        Throwable firstException = null;
        int size = synchronizations.size();
        for (int i = 0; i < size; i++) {
            try {
                synchronizations.get(i).afterCommit();
            } catch (Throwable thrown) {
                LOG.debug("Exception from afterCommit on " + synchronizations.get(i), thrown);
                if (firstException == null) {
                    firstException = thrown;
                }
            }
        }
        synchronizations = null;
        if (firstException != null) {
            throw JmsExceptionSupport.create(firstException);
        }
    }
}
