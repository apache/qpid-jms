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
import java.nio.BufferOverflowException;
import java.util.concurrent.ScheduledFuture;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
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
 * Represents the AMQP Transaction coordinator link used by the transaction context
 * of a session to control the lifetime of a given transaction.
 */
public class AmqpTransactionCoordinator extends AmqpAbstractResource<JmsSessionInfo, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionCoordinator.class);

    private static final Boolean ROLLBACK_MARKER = Boolean.FALSE;
    private static final Boolean COMMIT_MARKER = Boolean.TRUE;

    private final byte[] OUTBOUND_BUFFER = new byte[64];

    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator();

    private Delivery pendingDelivery;
    private AsyncResult pendingRequest;
    private ScheduledFuture<?> pendingTimeout;

    public AmqpTransactionCoordinator(JmsSessionInfo resourceInfo, Sender endpoint, AmqpResourceParent parent) {
        super(resourceInfo, endpoint, parent);
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider) throws IOException {
        try {
            if (pendingDelivery != null && pendingDelivery.remotelySettled()) {
                DeliveryState state = pendingDelivery.getRemoteState();
                JmsTransactionId txId = (JmsTransactionId) pendingDelivery.getContext();
                if (state instanceof Declared) {
                    LOG.debug("New TX started: {}", txId);
                    Declared declared = (Declared) state;
                    txId.setProviderHint(declared.getTxnId());
                    pendingRequest.onSuccess();
                } else if (state instanceof Rejected) {
                    LOG.debug("Last TX request failed: {}", txId);
                    Rejected rejected = (Rejected) state;
                    Exception cause = AmqpSupport.convertToException(getEndpoint(), rejected.getError());
                    JMSException failureCause = null;
                    if (txId.getProviderContext().equals(COMMIT_MARKER)) {
                        failureCause = new TransactionRolledBackException(cause.getMessage());
                    } else {
                        failureCause = new JMSException(cause.getMessage());
                    }

                    pendingRequest.onFailure(failureCause);
                } else {
                    LOG.debug("Last TX request succeeded: {}", txId);
                    pendingRequest.onSuccess();
                }

                // Reset state for next TX action.
                pendingDelivery.settle();
                pendingRequest = null;
                pendingDelivery = null;

                if (pendingTimeout != null) {
                    pendingTimeout.cancel(false);
                    pendingTimeout = null;
                }
            }

            super.processDeliveryUpdates(provider);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public void declare(JmsTransactionId txId, AsyncResult request) throws Exception {
        if (txId.getProviderHint() != null) {
            throw new IllegalStateException("Declar called while a TX is still Active.");
        }

        if (isClosed()) {
            request.onFailure(new JMSException("Cannot start new transaction: Coordinator remotely closed"));
            return;
        }

        Message message = Message.Factory.create();
        Declare declare = new Declare();
        message.setBody(new AmqpValue(declare));

        pendingDelivery = getEndpoint().delivery(tagGenerator.getNextTag());
        pendingDelivery.setContext(txId);
        pendingRequest = request;

        scheduleTimeoutIfNeeded("Timed out waiting for declare of new TX.");

        sendTxCommand(message);
    }

    public void discharge(JmsTransactionId txId, AsyncResult request, boolean commit) throws Exception {
        if (txId.getProviderHint() == null) {
            throw new IllegalStateException("Discharge called with no active Transaction.");
        }

        if (isClosed()) {
            Exception failureCause = null;

            if (commit) {
                failureCause = new TransactionRolledBackException("Transaction inbout: Coordinator remotely closed");
            } else {
                failureCause = new JMSException("Rollback cannot complete: Coordinator remotely closed");
            }

            request.onFailure(failureCause);
            return;
        }

        // Store the context of this action in the transaction ID for later completion.
        txId.setProviderContext(commit ? COMMIT_MARKER : ROLLBACK_MARKER);

        Message message = Message.Factory.create();
        Discharge discharge = new Discharge();
        discharge.setFail(!commit);
        discharge.setTxnId((Binary) txId.getProviderHint());
        message.setBody(new AmqpValue(discharge));

        pendingDelivery = getEndpoint().delivery(tagGenerator.getNextTag());
        pendingDelivery.setContext(txId);
        pendingRequest = request;

        scheduleTimeoutIfNeeded("Timed out waiting for discharge of TX.");

        sendTxCommand(message);
    }

    //----- Base class overrides ---------------------------------------------//

    @Override
    public void remotelyClosed(AmqpProvider provider) {

        Exception txnError = AmqpSupport.convertToException(getEndpoint(), getEndpoint().getRemoteCondition());

        // Alert any pending operation that the link failed to complete the pending
        // begin / commit / rollback operation.
        if (pendingRequest != null) {
            pendingRequest.onFailure(txnError);
            pendingRequest = null;
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

    private void scheduleTimeoutIfNeeded(String cause) {
        AmqpProvider provider = getParent().getProvider();
        if (provider.getRequestTimeout() != JmsConnectionInfo.INFINITE) {
            provider.scheduleRequestTimeout(pendingRequest, provider.getRequestTimeout(), new JmsOperationTimedOutException(cause));
        }
    }

    private void sendTxCommand(Message message) throws IOException {
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
