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

import java.nio.BufferOverflowException;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.AmqpTransactionContext.DischargeCompletion;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderIllegalStateException;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderTransactionInDoubtException;
import org.apache.qpid.jms.provider.exceptions.ProviderTransactionRolledBackException;
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
 * Represents the AMQP Transaction coordinator link used by the transaction context
 * of a session to control the lifetime of a given transaction.
 */
public class AmqpTransactionCoordinator extends AmqpAbstractResource<JmsSessionInfo, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionCoordinator.class);

    private static final Boolean ROLLBACK_MARKER = Boolean.FALSE;
    private static final Boolean COMMIT_MARKER = Boolean.TRUE;

    private final byte[] OUTBOUND_BUFFER = new byte[64];

    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator();

    public AmqpTransactionCoordinator(JmsSessionInfo resourceInfo, Sender endpoint, AmqpResourceParent parent) {
        super(resourceInfo, endpoint, parent);
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider, Delivery delivery) throws ProviderException {

        try {
            if (delivery != null && delivery.remotelySettled()) {
                DeliveryState state = delivery.getRemoteState();

                if (delivery.getContext() == null || !(delivery.getContext() instanceof OperationContext)) {
                    return;
                }

                OperationContext context = (OperationContext) delivery.getContext();

                AsyncResult pendingRequest = context.getRequest();
                JmsTransactionId txId = context.getTransactionId();

                if (state instanceof Declared) {
                    LOG.debug("New TX started: {}", txId);
                    Declared declared = (Declared) state;
                    txId.setProviderHint(declared.getTxnId());
                    pendingRequest.onSuccess();
                } else if (state instanceof Rejected) {
                    LOG.debug("Last TX request failed: {}", txId);
                    Rejected rejected = (Rejected) state;
                    ProviderException cause = AmqpSupport.convertToNonFatalException(getParent().getProvider(), getEndpoint(), rejected.getError());
                    if (COMMIT_MARKER.equals(txId.getProviderContext()) && !(cause instanceof ProviderTransactionRolledBackException)){
                        cause = new ProviderTransactionRolledBackException(cause.getMessage(), cause);
                    } else {
                        cause = new ProviderTransactionInDoubtException(cause.getMessage(), cause);
                    }

                    txId.setProviderHint(null);
                    pendingRequest.onFailure(cause);
                } else {
                    LOG.debug("Last TX request succeeded: {}", txId);
                    pendingRequest.onSuccess();
                }

                // Reset state for next TX action.
                delivery.settle();
                pendingRequest = null;

                if (context.getTimeout() != null) {
                    context.getTimeout().cancel(false);
                }
            }

            super.processDeliveryUpdates(provider, delivery);
        } catch (Throwable e) {
            throw ProviderExceptionSupport.createNonFatalOrPassthrough(e);
        }
    }

    public void declare(JmsTransactionId txId, AsyncResult request) throws ProviderException {

        if (isClosed()) {
            request.onFailure(new ProviderIllegalStateException("Cannot start new transaction: Coordinator remotely closed"));
            return;
        }

        if (txId.getProviderHint() != null) {
            throw new ProviderIllegalStateException("Declar called while a TX is still Active.");
        }

        Message message = Message.Factory.create();
        Declare declare = new Declare();
        message.setBody(new AmqpValue(declare));

        ScheduledFuture<?> timeout = scheduleTimeoutIfNeeded("Timed out waiting for declare of TX.", request);
        OperationContext context = new OperationContext(txId, request, timeout);

        Delivery delivery = getEndpoint().delivery(tagGenerator.getNextTag());
        delivery.setContext(context);

        sendTxCommand(message);
    }

    public void discharge(JmsTransactionId txId, DischargeCompletion request) throws ProviderException {

        if (isClosed()) {
            ProviderException failureCause = null;

            if (request.isCommit()) {
                failureCause = new ProviderTransactionRolledBackException("Transaction inbout: Coordinator remotely closed");
            } else {
                failureCause = new ProviderIllegalStateException("Rollback cannot complete: Coordinator remotely closed");
            }

            request.onFailure(failureCause);
            return;
        }

        if (txId.getProviderHint() == null) {
            throw new ProviderIllegalStateException("Discharge called with no active Transaction.");
        }

        // Store the context of this action in the transaction ID for later completion.
        txId.setProviderContext(request.isCommit() ? COMMIT_MARKER : ROLLBACK_MARKER);

        Message message = Message.Factory.create();
        Discharge discharge = new Discharge();
        discharge.setFail(!request.isCommit());
        discharge.setTxnId((Binary) txId.getProviderHint());
        message.setBody(new AmqpValue(discharge));

        ScheduledFuture<?> timeout = scheduleTimeoutIfNeeded("Timed out waiting for discharge of TX.", request);
        OperationContext context = new OperationContext(txId, request, timeout);

        Delivery delivery = getEndpoint().delivery(tagGenerator.getNextTag());
        delivery.setContext(context);

        sendTxCommand(message);
    }

    //----- Base class overrides ---------------------------------------------//

    @Override
    public void closeResource(AmqpProvider provider, ProviderException cause, boolean localClose) {

        // Alert any pending operation that the link failed to complete the pending
        // begin / commit / rollback operation.
        Delivery pending = getEndpoint().head();
        while (pending != null) {
            Delivery nextPending = pending.next();
            if (pending.getContext() != null && pending.getContext() instanceof OperationContext) {
                OperationContext context = (OperationContext) pending.getContext();
                context.request.onFailure(cause);
            }

            pending = nextPending;
        }

        // Override the base class version because we do not want to propagate
        // an error up to the client if remote close happens as that is an
        // acceptable way for the remote to indicate the discharge could not
        // be applied.

        if (getParent() != null) {
            getParent().removeChildResource(this);
        }

        if (getEndpoint() != null) {
            getEndpoint().close();
            getEndpoint().free();
        }

        LOG.debug("Transaction Coordinator link {} was remotely closed", getResourceInfo());
    }

    //----- Internal implementation ------------------------------------------//

    private class OperationContext {

        private final AsyncResult request;
        private final ScheduledFuture<?> timeout;
        private final JmsTransactionId transactionId;

        public OperationContext(JmsTransactionId transactionId, AsyncResult request, ScheduledFuture<?> timeout) {
            this.transactionId = transactionId;
            this.request = request;
            this.timeout = timeout;
        }

        public JmsTransactionId getTransactionId() {
            return transactionId;
        }

        public AsyncResult getRequest() {
            return request;
        }

        public ScheduledFuture<?> getTimeout() {
            return timeout;
        }
    }

    private ScheduledFuture<?> scheduleTimeoutIfNeeded(String cause, AsyncResult pendingRequest) {
        AmqpProvider provider = getParent().getProvider();
        if (provider.getRequestTimeout() != JmsConnectionInfo.INFINITE) {
            return provider.scheduleRequestTimeout(pendingRequest, provider.getRequestTimeout(), new ProviderOperationTimedOutException(cause));
        } else {
            return null;
        }
    }

    private void sendTxCommand(Message message) throws ProviderException {
        int encodedSize = 0;
        byte[] buffer = OUTBOUND_BUFFER;
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
