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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.JMSException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsResourceId;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderSynchronization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the details of a Session operating inside of a local JMS transaction.
 */
public class JmsLocalTransactionContext implements JmsTransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(JmsLocalTransactionContext.class);

    private final List<JmsTransactionSynchronization> synchronizations = new ArrayList<JmsTransactionSynchronization>();
    private final Map<JmsResourceId, JmsResourceId> participants = new HashMap<JmsResourceId, JmsResourceId>();
    private final JmsSession session;
    private final JmsConnection connection;
    private volatile JmsTransactionId transactionId;
    private volatile boolean failed;
    private JmsTransactionListener listener;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public JmsLocalTransactionContext(JmsSession session) {
        this.session = session;
        this.connection = session.getConnection();
    }

    @Override
    public void send(JmsConnection connection, final JmsOutboundMessageDispatch envelope) throws JMSException {
        lock.readLock().lock();
        try {
            if (isFailed()) {
                return;
            }

            // Use the completion callback to remove the need for a sync point.
            connection.send(envelope, new ProviderSynchronization() {

                @Override
                public void onPendingSuccess() {
                    LOG.trace("TX:{} has performed a send.", getTransactionId());
                    participants.put(envelope.getProducerId(), envelope.getProducerId());
                }

                @Override
                public void onPendingFailure(Throwable cause) {
                    LOG.trace("TX:{} has a failed send.", getTransactionId());
                    participants.put(envelope.getProducerId(), envelope.getProducerId());
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
                    public void onPendingFailure(Throwable cause) {
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
    public void addSynchronization(JmsTransactionSynchronization sync) throws JMSException {
        lock.writeLock().lock();
        try {
            if (sync.validate(this)) {
                synchronizations.add(sync);
            }
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean isFailed() {
        return failed;
    }

    @Override
    public void begin() throws JMSException {
        lock.writeLock().lock();
        try {
            if (!isInTransaction()) {
                reset();
                transactionId = connection.getNextTransactionId();
                JmsTransactionInfo transaction = new JmsTransactionInfo(session.getSessionId(), transactionId);
                connection.createResource(transaction);

                if (listener != null) {
                    try {
                        listener.onTransactionStarted();
                    } catch (Throwable error) {
                        LOG.trace("Local TX listener error ignored: {}", error);
                    }
                }

                LOG.debug("Begin: {}", transactionId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void commit() throws JMSException {
        lock.writeLock().lock();
        try {
            if (isFailed()) {
                try {
                    rollback();
                } catch (Exception e) {
                    LOG.trace("Error during rollback of failed TX: {}", e);
                }
                throw new TransactionRolledBackException("Transaction failed and has been rolled back.");
            } else {
                LOG.debug("Commit: {} syncCount: {}", transactionId,
                          (synchronizations != null ? synchronizations.size() : 0));

                JmsTransactionId oldTransactionId = this.transactionId;
                try {
                    connection.commit(session.getSessionId(), new ProviderSynchronization() {

                        @Override
                        public void onPendingSuccess() {
                            reset();
                        }

                        @Override
                        public void onPendingFailure(Throwable cause) {
                            reset();
                        }
                    });

                    if (listener != null) {
                        try {
                            listener.onTransactionCommitted();
                        } catch (Throwable error) {
                            LOG.trace("Local TX listener error ignored: {}", error);
                        }
                    }
                    afterCommit();
                } catch (JMSException cause) {
                    LOG.info("Commit failed for transaction: {}", oldTransactionId);
                    if (listener != null) {
                        try {
                            listener.onTransactionRolledBack();
                        } catch (Throwable error) {
                            LOG.trace("Local TX listener error ignored: {}", error);
                        }
                    }
                    afterRollback();
                    throw cause;
                } finally {
                    LOG.debug("Commit starting new TX after commit completed.");
                    begin();
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
            LOG.debug("Rollback: {} syncCount: {}", transactionId,
                      (synchronizations != null ? synchronizations.size() : 0));
            try {
                connection.rollback(session.getSessionId(), new ProviderSynchronization() {

                    @Override
                    public void onPendingSuccess() {
                        reset();
                    }

                    @Override
                    public void onPendingFailure(Throwable cause) {
                        reset();
                    }
                });

                if (listener != null) {
                    try {
                        listener.onTransactionRolledBack();
                    } catch (Throwable error) {
                        LOG.trace("Local TX listener error ignored: {}", error);
                    }
                }

                afterRollback();
            } finally {
                if (startNewTx) {
                    LOG.debug("Rollback starting new TX after rollback completed.");
                    begin();
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
            failed = !participants.isEmpty();
        } finally {
            if (lock.writeLock().isHeldByCurrentThread()) {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void onConnectionRecovery(Provider provider) throws Exception {
        // Attempt to hold the write lock so that new work gets blocked while we take
        // care of the recovery, if we can't get it not critical but this prevents
        // work from getting queued on the provider needlessly.
        lock.writeLock().tryLock();
        try {

            // On connection recover we open a new TX to replace the existing one.
            transactionId = connection.getNextTransactionId();
            JmsTransactionInfo transaction = new JmsTransactionInfo(session.getSessionId(), transactionId);
            ProviderFuture request = new ProviderFuture();
            provider.create(transaction, request);
            request.sync();

            // It is ok to use the newly created TX from here if the TX never had any
            // work done within it otherwise we want the next commit to fail.
            failed = !participants.isEmpty();
        } finally {
            if (lock.writeLock().isHeldByCurrentThread()) {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public String toString() {
        return "JmsLocalTransactionContext{ transactionId=" + getTransactionId() + " }";
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
        lock.readLock().lock();
        try {
            return transactionId != null;
        } finally {
            lock.readLock().unlock();
        }
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
        transactionId = null;
        failed = false;
        participants.clear();
    }

    /*
     * Must be called with the write lock held to ensure the synchronizations list
     * can be safely cleared.
     */
    private void afterRollback() throws JMSException {
        if (synchronizations.isEmpty()) {
            return;
        }

        Throwable firstException = null;
        for (JmsTransactionSynchronization sync : synchronizations) {
            try {
                sync.afterRollback();
            } catch (Throwable thrown) {
                LOG.debug("Exception from afterRollback on " + sync, thrown);
                if (firstException == null) {
                    firstException = thrown;
                }
            }
        }

        synchronizations.clear();
        if (firstException != null) {
            throw JmsExceptionSupport.create(firstException);
        }
    }

    /*
     * Must be called with the write lock held to ensure the synchronizations list
     * can be safely cleared.
     */
    private void afterCommit() throws JMSException {
        if (synchronizations.isEmpty()) {
            return;
        }

        Throwable firstException = null;
        for (JmsTransactionSynchronization sync : synchronizations) {
            try {
                sync.afterCommit();
            } catch (Throwable thrown) {
                LOG.debug("Exception from afterCommit on " + sync, thrown);
                if (firstException == null) {
                    firstException = thrown;
                }
            }
        }

        synchronizations.clear();
        if (firstException != null) {
            throw JmsExceptionSupport.create(firstException);
        }
    }
}
