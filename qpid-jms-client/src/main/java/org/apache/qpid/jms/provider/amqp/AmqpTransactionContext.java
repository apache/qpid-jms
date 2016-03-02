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
package org.apache.qpid.jms.provider.amqp;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.jms.IllegalStateException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.amqp.builders.AmqpTransactionCoordinatorBuilder;
import org.apache.qpid.proton.amqp.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the operations surrounding AMQP transaction control.
 *
 * The Transaction will carry a JmsTransactionId while the Transaction is open, once a
 * transaction has been committed or rolled back the Transaction Id is cleared.
 */
public class AmqpTransactionContext implements AmqpResourceParent {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionContext.class);

    private final AmqpSession session;
    private final Set<AmqpConsumer> txConsumers = new LinkedHashSet<AmqpConsumer>();

    private JmsTransactionId current;
    private AmqpTransactionCoordinator coordinator;

    /**
     * Creates a new AmqpTransactionContext instance.
     *
     * @param session
     *        The session that owns this transaction context.
     * @param resourceInfo
     *        The resourceInfo that defines this transaction context.
     */
    public AmqpTransactionContext(AmqpSession session, JmsSessionInfo resourceInfo) {
        this.session = session;
    }

    public void begin(final JmsTransactionId txId, final AsyncResult request) throws Exception {
        if (current != null) {
            throw new IOException("Begin called while a TX is still Active.");
        }

        final AsyncResult declareCompletion = new AsyncResult() {

            @Override
            public void onSuccess() {
                current = txId;
                request.onSuccess();
            }

            @Override
            public void onFailure(Throwable result) {
                current = null;
                request.onFailure(result);
            }

            @Override
            public boolean isComplete() {
                return current != null;
            }
        };


        if (coordinator == null || coordinator.isClosed()) {
            AmqpTransactionCoordinatorBuilder builder =
                new AmqpTransactionCoordinatorBuilder(this, session.getResourceInfo());
            builder.buildResource(new AsyncResult() {

                @Override
                public void onSuccess() {
                    try {
                        coordinator.declare(txId, declareCompletion);
                    } catch (Exception e) {
                        request.onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable result) {
                    request.onFailure(result);
                }

                @Override
                public boolean isComplete() {
                    return request.isComplete();
                }
            });
        } else {
            coordinator.declare(txId, declareCompletion);
        }
    }

    public void commit(JmsTransactionInfo transactionInfo, final AsyncResult request) throws Exception {
        if (!transactionInfo.getId().equals(current)) {
            if (!transactionInfo.isInDoubt() && current == null) {
                throw new IllegalStateException("Commit called with no active Transaction.");
            } else if (!transactionInfo.isInDoubt() && current != null) {
                throw new IllegalStateException("Attempt to Commit a transaction other than the current one");
            } else {
                throw new TransactionRolledBackException("Transaction in doubt and cannot be committed.");
            }
        }

        preCommit();

        LOG.trace("TX Context[{}] committing current TX[[]]", this, current);
        coordinator.discharge(current, new AsyncResult() {

            @Override
            public void onSuccess() {
                current = null;
                postCommit();
                request.onSuccess();
            }

            @Override
            public void onFailure(Throwable result) {
                current = null;
                postCommit();
                request.onFailure(result);
            }

            @Override
            public boolean isComplete() {
                return current == null;
            }

        }, true);
    }

    public void rollback(JmsTransactionInfo transactionInfo, final AsyncResult request) throws Exception {
        if (!transactionInfo.getId().equals(current)) {
            if (!transactionInfo.isInDoubt() && current == null) {
                throw new IllegalStateException("Rollback called with no active Transaction.");
            } else if (!transactionInfo.isInDoubt() && current != null) {
                throw new IllegalStateException("Attempt to rollback a transaction other than the current one");
            } else {
                request.onSuccess();
                return;
            }
        }

        preRollback();

        LOG.trace("TX Context[{}] rolling back current TX[[]]", this, current);
        coordinator.discharge(current, new AsyncResult() {

            @Override
            public void onSuccess() {
                current = null;
                postRollback();
                request.onSuccess();
            }

            @Override
            public void onFailure(Throwable result) {
                current = null;
                postRollback();
                request.onFailure(result);
            }

            @Override
            public boolean isComplete() {
                return current == null;
            }

        }, false);
    }

    //----- Context utility methods ------------------------------------------//

    public void registerTxConsumer(AmqpConsumer consumer) {
        txConsumers.add(consumer);
    }

    public AmqpSession getSession() {
        return session;
    }

    public JmsTransactionId getTransactionId() {
        return current;
    }

    public boolean isTransactionFailed() {
        return coordinator == null ? false : coordinator.isClosed();
    }

    public Binary getAmqpTransactionId() {
        Binary result = null;
        if (current != null) {
            result = (Binary) current.getProviderHint();
        }
        return result;
    }

    @Override
    public String toString() {
        return this.session.getSessionId() + ": txContext";
    }

    //----- Transaction pre / post completion --------------------------------//

    private void preCommit() {
        for (AmqpConsumer consumer : txConsumers) {
            consumer.preCommit();
        }
    }

    private void preRollback() {
        for (AmqpConsumer consumer : txConsumers) {
            consumer.preRollback();
        }
    }

    private void postCommit() {
        for (AmqpConsumer consumer : txConsumers) {
            consumer.postCommit();
        }

        txConsumers.clear();
    }

    private void postRollback() {
        for (AmqpConsumer consumer : txConsumers) {
            consumer.postRollback();
        }

        txConsumers.clear();
    }

    //----- Resource Parent implementation -----------------------------------//

    @Override
    public void addChildResource(AmqpResource resource) {
        if (resource instanceof AmqpTransactionCoordinator) {
            coordinator = (AmqpTransactionCoordinator) resource;
        }
    }

    @Override
    public void removeChildResource(AmqpResource resource) {
        // We don't clear the coordinator link so that we can refer to it
        // to check if the current TX has failed due to link closed during
        // normal operations.
    }

    @Override
    public AmqpProvider getProvider() {
        return session.getProvider();
    }
}
