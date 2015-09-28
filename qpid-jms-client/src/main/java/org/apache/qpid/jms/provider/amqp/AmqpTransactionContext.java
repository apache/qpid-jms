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
package org.apache.qpid.jms.provider.amqp;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.jms.IllegalStateException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the operations surrounding AMQP transaction control.
 *
 * The Transaction will carry a JmsTransactionId while the Transaction is open, once a
 * transaction has been committed or rolled back the Transaction Id is cleared.
 */
public class AmqpTransactionContext extends AmqpAbstractResource<JmsSessionInfo, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionContext.class);

    private static final Boolean ROLLBACK_MARKER = Boolean.FALSE;
    private static final Boolean COMMIT_MARKER = Boolean.TRUE;

    private final AmqpSession session;
    private JmsTransactionId current;
    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator();
    private final Set<AmqpConsumer> txConsumers = new LinkedHashSet<AmqpConsumer>();

    private Delivery pendingDelivery;
    private AsyncResult pendingRequest;

    /**
     * Creates a new AmqpTransactionContext instance.
     *
     * @param session
     *        The session that owns this transaction context.
     * @param resourceInfo
     *        The resourceInfo that defines this transaction context.
     * @param sender
     *        The local sender endpoint for this transaction context.
     */
    public AmqpTransactionContext(AmqpSession session, JmsSessionInfo resourceInfo, Sender sender) {
        super(resourceInfo, sender, session);

        this.session = session;
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider) throws IOException {
        try {
            if (pendingDelivery != null && pendingDelivery.remotelySettled()) {
                DeliveryState state = pendingDelivery.getRemoteState();
                if (state instanceof Declared) {
                    Declared declared = (Declared) state;
                    current.setProviderHint(declared.getTxnId());
                    pendingDelivery.settle();
                    LOG.debug("New TX started: {}", current.getProviderHint());
                    AsyncResult request = this.pendingRequest;
                    this.pendingRequest = null;
                    this.pendingDelivery = null;
                    request.onSuccess();
                } else if (state instanceof Rejected) {
                    LOG.debug("Last TX request failed: {}", current.getProviderHint());
                    pendingDelivery.settle();
                    Rejected rejected = (Rejected) state;
                    TransactionRolledBackException ex =
                        new TransactionRolledBackException(rejected.getError().getDescription());
                    AsyncResult request = this.pendingRequest;
                    this.current = null;
                    this.pendingRequest = null;
                    this.pendingDelivery = null;
                    postRollback();
                    request.onFailure(ex);
                } else {
                    LOG.debug("Last TX request succeeded: {}", current.getProviderHint());
                    pendingDelivery.settle();
                    AsyncResult request = this.pendingRequest;
                    if (pendingDelivery.getContext() != null) {
                        if (pendingDelivery.getContext().equals(COMMIT_MARKER)) {
                            postCommit();
                        } else {
                            postRollback();
                        }
                    }
                    this.current = null;
                    this.pendingRequest = null;
                    this.pendingDelivery = null;
                    request.onSuccess();
                }
            }

            super.processDeliveryUpdates(provider);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public void begin(JmsTransactionId txId, AsyncResult request) throws Exception {
        if (current != null) {
            throw new IOException("Begin called while a TX is still Active.");
        }

        Message message = Message.Factory.create();
        Declare declare = new Declare();
        message.setBody(new AmqpValue(declare));

        pendingDelivery = getEndpoint().delivery(tagGenerator.getNextTag());
        pendingRequest = request;
        current = txId;

        sendTxCommand(message);
    }

    public void commit(AsyncResult request) throws Exception {
        if (current == null) {
            throw new IllegalStateException("Commit called with no active Transaction.");
        }

        preCommit();

        Message message = Message.Factory.create();
        Discharge discharge = new Discharge();
        discharge.setFail(false);
        discharge.setTxnId((Binary) current.getProviderHint());
        message.setBody(new AmqpValue(discharge));

        pendingDelivery = getEndpoint().delivery(tagGenerator.getNextTag());
        pendingDelivery.setContext(COMMIT_MARKER);
        pendingRequest = request;

        sendTxCommand(message);
    }

    public void rollback(AsyncResult request) throws Exception {
        if (current == null) {
            throw new IllegalStateException("Rollback called with no active Transaction.");
        }

        preRollback();

        Message message = Message.Factory.create();
        Discharge discharge = new Discharge();
        discharge.setFail(true);
        discharge.setTxnId((Binary) current.getProviderHint());
        message.setBody(new AmqpValue(discharge));

        pendingDelivery = getEndpoint().delivery(tagGenerator.getNextTag());
        pendingDelivery.setContext(ROLLBACK_MARKER);
        pendingRequest = request;

        sendTxCommand(message);
    }

    public void registerTxConsumer(AmqpConsumer consumer) {
        this.txConsumers.add(consumer);
    }

    public AmqpSession getSession() {
        return this.session;
    }

    public JmsTransactionId getTransactionId() {
        return this.current;
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

    private void preCommit() throws Exception {
        for (AmqpConsumer consumer : txConsumers) {
            consumer.preCommit();
        }
    }

    private void preRollback() throws Exception {
        for (AmqpConsumer consumer : txConsumers) {
            consumer.preRollback();
        }
    }

    private void postCommit() throws Exception {
        for (AmqpConsumer consumer : txConsumers) {
            consumer.postCommit();
        }

        txConsumers.clear();
    }

    private void postRollback() throws Exception {
        for (AmqpConsumer consumer : txConsumers) {
            consumer.postRollback();
        }

        txConsumers.clear();
    }

    private void sendTxCommand(Message message) throws IOException {
        int encodedSize = 0;
        byte[] buffer = new byte[4 * 1024];
        while (true) {
            try {
                encodedSize = message.encode(buffer, 0, buffer.length);
                break;
            } catch (BufferOverflowException e) {
                buffer = new byte[buffer.length * 2];
            }
        }

        Sender sender = getEndpoint();
        sender.send(buffer, 0, encodedSize);
        sender.advance();
    }
}
