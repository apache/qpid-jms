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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.JMSException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
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

    private final List<JmsTxSynchronization> synchronizations = new ArrayList<JmsTxSynchronization>();
    private final JmsSession session;
    private final JmsConnection connection;
    private JmsTransactionId transactionId;
    private volatile boolean failed;
    private volatile boolean hasWork;
    private JmsTransactionListener listener;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public JmsLocalTransactionContext(JmsSession session) {
        this.session = session;
        this.connection = session.getConnection();
    }

    @Override
    public void send(JmsConnection connection, JmsOutboundMessageDispatch envelope) throws JMSException {
        lock.readLock().lock();
        try {
            if (isFailed()) {
                return;
            }

            // Use the completion callback to remove the need for a sync point.
            connection.send(envelope, new ProviderSynchronization() {

                @Override
                public void onPendingSuccess() {
                    hasWork = true;
                }

                @Override
                public void onPendingFailure(Throwable cause) {
                    hasWork = true;
                }
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void acknowledge(JmsConnection connection, JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException {
        // Consumed or delivered messages fall into a transaction otherwise just pass it in.
        if (ackType == ACK_TYPE.CONSUMED || ackType == ACK_TYPE.DELIVERED) {
            lock.readLock().lock();
            try {
                connection.acknowledge(envelope, ackType, new ProviderSynchronization() {

                    @Override
                    public void onPendingSuccess() {
                        hasWork = true;
                    }

                    @Override
                    public void onPendingFailure(Throwable cause) {
                        hasWork = true;
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
    public void addSynchronization(JmsTxSynchronization sync) throws JMSException {
        lock.readLock().lock();
        try {
            if (sync.validate(this)) {
                synchronizations.add(sync);
            }
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        } finally {
            lock.readLock().unlock();
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
            connection.rollback(session.getSessionId(), new ProviderSynchronization() {

                @Override
                public void onPendingSuccess() {
                    reset();
                }

                @Override
                public void onPendingFailure(Throwable cause) {
                }
            });

            if (listener != null) {
                try {
                    listener.onTransactionRolledBack();
                } catch (Throwable error) {
                    LOG.trace("Local TX listener error ignored: {}", error);
                }
            }

            try {
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
            failed = true;
        } finally {
            if (lock.writeLock().isHeldByCurrentThread()) {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void onConnectionRecovery(Provider provider) throws Exception {
        if (lock.writeLock().tryLock()) {
            try {
                if (isInTransaction()) {
                    LOG.info("onConnectionRecovery starting new transaction to replace old one.");

                    // On connection recover we open a new TX to replace the existing one.
                    transactionId = connection.getNextTransactionId();
                    JmsTransactionInfo transaction = new JmsTransactionInfo(session.getSessionId(), transactionId);
                    ProviderFuture request = new ProviderFuture();
                    provider.create(transaction, request);
                    request.sync();

                    LOG.info("onConnectionRecovery started new transaction to replace old one.");

                    // It is ok to use the newly created TX from here if the TX never had any
                    // work done within it.
                    if (!hasWork) {
                        failed = false;
                    }
                }
            } finally {
                if (lock.writeLock().isHeldByCurrentThread()) {
                    lock.writeLock().unlock();
                }
            }
        } else {
            // Some transaction work was pending since we could not lock, mark the TX
            // as failed so that future calls will fail.
            if (transactionId != null) {
                LOG.info("onConnectionRecovery TX work pending, marking TX as failed.");
                failed = true;
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

    //------------- Implementation methods -----------------------------------//

    /*
     * Must be called with the write lock held to ensure the synchronizations list
     * can be safely cleared.
     */
    private void reset() {
        transactionId = null;
        failed = false;
        hasWork = false;
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
        for (JmsTxSynchronization sync : synchronizations) {
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
        for (JmsTxSynchronization sync : synchronizations) {
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
