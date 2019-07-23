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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.JMSException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsResourceId;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderSynchronization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the details of a Session operating inside of a local JMS transaction.
 */
public class JmsLocalTransactionContext implements JmsTransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(JmsLocalTransactionContext.class);

    private final Map<JmsResourceId, JmsResourceId> participants = new HashMap<JmsResourceId, JmsResourceId>();
    private final JmsSession session;
    private final JmsConnection connection;
    private JmsTransactionInfo transactionInfo;
    private JmsTransactionListener listener;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public JmsLocalTransactionContext(JmsSession session) {
        this.session = session;
        this.connection = session.getConnection();
    }

    @Override
    public void send(JmsConnection connection, final JmsOutboundMessageDispatch envelope, ProviderSynchronization outcome) throws JMSException {
        lock.readLock().lock();
        try {
            if (isInDoubt()) {
                // Need to signal that the request is going to pass before completing
                if (outcome != null) {
                    outcome.onPendingSuccess();
                }
                // Ensure that asynchronous completions get signaled while TX is in doubt
                if (envelope.isCompletionRequired()) {
                    connection.onCompletedMessageSend(envelope);
                }
                return;
            }

            // Use the completion callback to remove the need for a sync point.
            connection.send(envelope, new ProviderSynchronization() {

                @Override
                public void onPendingSuccess() {
                    LOG.trace("TX:{} has performed a send.", getTransactionId());
                    participants.put(envelope.getProducerId(), envelope.getProducerId());
                    if (outcome != null) {
                        outcome.onPendingSuccess();
                    }
                }

                @Override
                public void onPendingFailure(ProviderException cause) {
                    LOG.trace("TX:{} has a failed send.", getTransactionId());
                    participants.put(envelope.getProducerId(), envelope.getProducerId());
                    if (outcome != null) {
                        outcome.onPendingFailure(cause);
                    }
                }
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void acknowledge(JmsConnection connection, final JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        // Consumed or delivered messages fall into a transaction otherwise just pass it in.
        if (ackType == ACK_TYPE.ACCEPTED || ackType == ACK_TYPE.DELIVERED) {
            lock.readLock().lock();
            try {
                connection.acknowledge(envelope, ackType, new ProviderSynchronization() {

                    @Override
                    public void onPendingSuccess() {
                        LOG.trace("TX:{} has performed a acknowledge.", getTransactionId());
                        participants.put(envelope.getConsumerId(), envelope.getConsumerId());
                    }

                    @Override
                    public void onPendingFailure(ProviderException cause) {
                        LOG.trace("TX:{} has failed a acknowledge.", getTransactionId());
                        participants.put(envelope.getConsumerId(), envelope.getConsumerId());
                    }
                });
            } finally {
                lock.readLock().unlock();
            }
        } else {
            connection.acknowledge(envelope, ackType);
        }
    }

    @Override
    public boolean isInDoubt() {
        return transactionInfo != null ? transactionInfo.isInDoubt() : false;
    }

    @Override
    public void begin() throws JMSException {
        lock.writeLock().lock();
        try {
            reset();
            final JmsTransactionInfo transactionInfo = getNextTransactionInfo();
            connection.createResource(transactionInfo, new ProviderSynchronization() {

                @Override
                public void onPendingSuccess() {
                    JmsLocalTransactionContext.this.transactionInfo = transactionInfo;
                }

                @Override
                public void onPendingFailure(ProviderException cause) {
                    JmsLocalTransactionContext.this.transactionInfo = transactionInfo;
                    transactionInfo.setInDoubt(true);
                }
            });

            if (listener != null) {
                try {
                    listener.onTransactionStarted();
                } catch (Throwable error) {
                    LOG.trace("Local TX listener error ignored: {}", error);
                }
            }

            LOG.debug("Begin: {}", transactionInfo.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void commit() throws JMSException {
        lock.writeLock().lock();
        try {
            if (isInDoubt()) {
                try {
                    rollback();
                } catch (Exception e) {
                    LOG.trace("Error during rollback of failed TX: {}", e);
                }
                throw new TransactionRolledBackException("Transaction failed and has been rolled back.");
            } else {
                LOG.debug("Commit: {}", transactionInfo.getId());

                JmsTransactionId oldTransactionId = transactionInfo.getId();
                final JmsTransactionInfo nextTx = getNextTransactionInfo();

                try {
                    connection.commit(transactionInfo, nextTx, new ProviderSynchronization() {

                        @Override
                        public void onPendingSuccess() {
                            reset();
                            JmsLocalTransactionContext.this.transactionInfo = nextTx;
                        }

                        @Override
                        public void onPendingFailure(ProviderException cause) {
                            reset();
                            JmsLocalTransactionContext.this.transactionInfo = nextTx;
                        }
                    });

                    if (listener != null) {
                        try {
                            listener.onTransactionCommitted();
                        } catch (Throwable error) {
                            LOG.trace("Local TX listener error ignored: {}", error);
                        }

                        try {
                            listener.onTransactionStarted();
                        } catch (Throwable error) {
                            LOG.trace("Local TX listener error ignored: {}", error);
                        }
                    }
                } catch (JMSException cause) {
                    LOG.info("Commit failed for transaction: {}", oldTransactionId);
                    if (listener != null) {
                        try {
                            listener.onTransactionRolledBack();
                        } catch (Throwable error) {
                            LOG.trace("Local TX listener error ignored: {}", error);
                        }
                    }

                    throw cause;
                } finally {
                    try {
                        // If the provider failed to start a new transaction there will not be
                        // a current provider transaction id present, so we attempt to create
                        // one to recover our state.
                        if (nextTx.getId().getProviderTxId() == null) {
                            begin();
                        }
                    } catch (Exception e) {
                        // TODO
                        // At this point the transacted session is now unrecoverable, we should
                        // probably close it.
                        LOG.info("Failed to start new Transaction after failed rollback of: {}", oldTransactionId);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() throws JMSException {
        doRollback(true);
    }

    private void doRollback(boolean startNewTx) throws JMSException {
        lock.writeLock().lock();
        try {
            LOG.debug("Rollback: {}", transactionInfo.getId());
            JmsTransactionId oldTransactionId = transactionInfo.getId();
            final JmsTransactionInfo nextTx;
            if (startNewTx) {
                nextTx = getNextTransactionInfo();
            } else {
                nextTx = null;
            }

            try {
                connection.rollback(transactionInfo, nextTx, new ProviderSynchronization() {

                    @Override
                    public void onPendingSuccess() {
                        reset();
                        JmsLocalTransactionContext.this.transactionInfo = nextTx;
                    }

                    @Override
                    public void onPendingFailure(ProviderException cause) {
                        reset();
                        JmsLocalTransactionContext.this.transactionInfo = nextTx;
                    }
                });

                if (listener != null) {
                    try {
                        listener.onTransactionRolledBack();
                    } catch (Throwable error) {
                        LOG.trace("Local TX listener error ignored: {}", error);
                    }

                    try {
                        if (startNewTx) {
                            listener.onTransactionStarted();
                        }
                    } catch (Throwable error) {
                        LOG.trace("Local TX listener error ignored: {}", error);
                    }
                }
            } catch (JMSException cause) {
                LOG.info("Rollback failed for transaction: {}", oldTransactionId);
                if (listener != null) {
                    try {
                        listener.onTransactionRolledBack();
                    } catch (Throwable error) {
                        LOG.trace("Local TX listener error ignored: {}", error);
                    }
                }

                throw cause;
            } finally {
                try {
                    // If the provider failed to start a new transaction there will not be
                    // a current provider transaction id present, so we attempt to create
                    // one to recover our state.
                    if (startNewTx && nextTx.getId().getProviderTxId() == null) {
                        begin();
                    }
                } catch (Exception e) {
                    // TODO
                    // At this point the transacted session is now unrecoverable, we should
                    // probably close it.
                    LOG.info("Failed to start new Transaction after failed rollback of: {}", oldTransactionId);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void shutdown() throws JMSException {
        doRollback(false);
    }

    @Override
    public void onConnectionInterrupted() {
        lock.writeLock().tryLock();
        try {
            transactionInfo.setInDoubt(true);
        } finally {
            if (lock.writeLock().isHeldByCurrentThread()) {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void onConnectionRecovery(Provider provider) throws Exception {
        // If we get the lock then no TX commit / rollback / begin is in progress
        // otherwise one is and we can only assume that it should fail given the
        // connection was dropped.
        if (lock.writeLock().tryLock()) {
            try {
                // If we got the lock then there is no pending commit / rollback / begin so
                // we can safely create a new transaction, if there is work pending on the
                // current transaction we must mark it as in-doubt so that a commit attempt
                // will then roll it back.
                transactionInfo = getNextTransactionInfo();
                ProviderFuture request = provider.newProviderFuture(new ProviderSynchronization() {

                    @Override
                    public void onPendingSuccess() {
                        transactionInfo.setInDoubt(!participants.isEmpty());
                    }

                    @Override
                    public void onPendingFailure(ProviderException cause) {
                        transactionInfo.setInDoubt(true);
                    }
                });

                LOG.trace("Transaction recovery creating new TX:{} after failover.", transactionInfo.getId());

                provider.create(transactionInfo, request);
                request.sync();
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            LOG.trace("Transaction recovery marking current TX:{} as in-doubt.", transactionInfo.getId());

            // We did not get the lock so there is an operation in progress and our only
            // option is to mark the state as failed so a commit will roll back.
            transactionInfo.setInDoubt(true);
        }
    }

    @Override
    public String toString() {
        return "JmsLocalTransactionContext{ transactionId=" + getTransactionId() + " }";
    }

    //------------- Getters and Setters --------------------------------------//

    @Override
    public JmsTransactionId getTransactionId() {
        return transactionInfo.getId();
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
        return true;
    }

    @Override
    public boolean isActiveInThisContext(JmsResourceId resouceId) {
        lock.readLock().lock();
        try {
            return participants.containsKey(resouceId);
        } finally {
            lock.readLock().unlock();
        }
    }

    //------------- Implementation methods -----------------------------------//

    /*
     * Must be called with the write lock held to ensure the synchronizations list
     * can be safely cleared.
     */
    private void reset() {
        participants.clear();
    }

    private JmsTransactionInfo getNextTransactionInfo() {
        JmsTransactionId transactionId = connection.getNextTransactionId();
        return new JmsTransactionInfo(session.getSessionId(), transactionId);
    }
}
