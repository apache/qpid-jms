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

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the details of a Session operating inside of a local JMS transaction.
 */
public class JmsLocalTransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(JmsLocalTransactionContext.class);

    private List<JmsTxSynchronization> synchronizations;
    private final JmsSession session;
    private final JmsConnection connection;
    private JmsTransactionId transactionId;
    private JmsTransactionListener listener;

    public JmsLocalTransactionContext(JmsSession session) {
        this.session = session;
        this.connection = session.getConnection();
    }

    /**
     * Adds the given Transaction synchronization to the current list.
     *
     * @param synchronization
     *        the transaction synchronization to add.
     */
    public void addSynchronization(JmsTxSynchronization s) {
        if (synchronizations == null) {
            synchronizations = new ArrayList<JmsTxSynchronization>(10);
        }
        synchronizations.add(s);
    }

    /**
     * Clears the current Transacted state.  This is usually done when the client
     * detects that a failover has occurred and needs to create a new Transaction
     * for a Session that was previously enlisted in a transaction.
     */
    public void clear() {
        this.transactionId = null;
        this.synchronizations = null;
    }

    /**
     * Start a local transaction.
     *
     * @throws javax.jms.JMSException on internal error
     */
    public void begin() throws JMSException {
        if (transactionId == null) {
            synchronizations = null;

            transactionId = connection.getNextTransactionId();
            JmsTransactionInfo transaction = new JmsTransactionInfo(session.getSessionId(), transactionId);
            connection.createResource(transaction);

            if (listener != null) {
                listener.onTransactionStarted();
            }

            LOG.debug("Begin: {}", transactionId);
        }
    }

    /**
     * Rolls back any work done in this transaction and releases any locks
     * currently held.
     *
     * @throws JMSException
     *         if the JMS provider fails to roll back the transaction due to some internal error.
     */
    public void rollback() throws JMSException {
        if (transactionId != null) {
            LOG.debug("Rollback: {} syncCount: {}", transactionId,
                      (synchronizations != null ? synchronizations.size() : 0));

            transactionId = null;
            connection.rollback(session.getSessionId());

            if (listener != null) {
                listener.onTransactionRolledBack();
            }
        }

        afterRollback();
    }

    /**
     * Commits all work done in this transaction and releases any locks
     * currently held.
     *
     * @throws JMSException
     *         if the JMS provider fails to roll back the transaction due to some internal error.
     */
    public void commit() throws JMSException {
        if (transactionId != null) {
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
        return "JmsLocalTransactionContext{transactionId=" + transactionId + "}";
    }

    //------------- Getters and Setters --------------------------------------//

    public JmsTransactionId getTransactionId() {
        return this.transactionId;
    }

    public JmsTransactionListener getListener() {
        return listener;
    }

    public void setListener(JmsTransactionListener listener) {
        this.listener = listener;
    }

    public boolean isInTransaction() {
        return this.transactionId != null;
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
