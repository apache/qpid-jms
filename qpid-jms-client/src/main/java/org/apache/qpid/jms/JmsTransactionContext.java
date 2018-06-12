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
 * A Transaction Context is used to track and manage the state of a
 * Transaction in a JMS Session object.
 */
public interface JmsTransactionContext {

    /**
     * Allows the context to intercept a message acknowledgement and perform any
     * additional logic prior to the acknowledge being forwarded onto the connection.
     *
     * @param connection
     *        the connection that the acknowledge will be forwarded to.
     * @param envelope
     *        the envelope that contains the message to be acknowledged.
     * @param ackType
     *        the acknowledgement type being requested.
     *
     * @throws JMSException if an error occurs while performing the acknowledge.
     */
    void acknowledge(JmsConnection connection, JmsInboundMessageDispatch envelope, ACK_TYPE ackType) throws JMSException;

    /**
     * Allows the context to intercept and perform any additional logic
     * prior to a message being sent on to the connection and subsequently
     * the remote peer.
     *
     * @param connection
     *        the connection that will be do the send of the message
     * @param envelope
     *        the envelope that contains the message to be sent.
     * @param outcome
     * 	      Synchronization used to set state prior to completion of the send call.
     *
     * @throws JMSException if an error occurs during the send.
     */
    void send(JmsConnection connection, JmsOutboundMessageDispatch envelope, ProviderSynchronization outcome) throws JMSException;

    /**
     * @return if the currently transaction has been marked as being in an unknown state.
     */
    boolean isInDoubt();

    /**
     * Start a transaction if none is currently active.
     *
     * @throws JMSException on internal error occurs.
     */
    void begin() throws JMSException;

    /**
     * Rolls back any work done in this transaction and releases any locks
     * currently held.  If the current transaction is in a failed state this
     * resets that state and initiates a new transaction via a begin call.
     *
     * @throws JMSException
     *         if the JMS provider fails to roll back the transaction due to some internal error.
     */
    void rollback() throws JMSException;

    /**
     * Commits all work done in this transaction and releases any locks
     * currently held.  If the transaction is in a failed state this method
     * throws an exception to indicate that the transaction has failed and
     * will be rolled back a new transaction is started via a begin call.
     *
     * @throws JMSException
     *         if the commit fails to roll back the transaction due to some internal error.
     */
    void commit() throws JMSException;

    /**
     * Rolls back any work done in this transaction and releases any locks
     * currently held.  This method will not start a new transaction and no new
     * transacted work should be done using this transaction.
     *
     * @throws JMSException
     *         if the JMS provider fails to roll back the transaction due to some internal error.
     */
    void shutdown() throws JMSException;

    /**
     * @return the transaction ID of the currently active TX or null if none active.
     */
    JmsTransactionId getTransactionId();

    /**
     * @return the currently configured JMS Transaction listener which will receive TX events.
     */
    JmsTransactionListener getListener();

    /**
     * Sets the single JMS Transaction listener which will be notified of significant TX events
     * such as Commit or Rollback.
     *
     * @param listener
     *        the JMS Transaction listener that will be sent all TX event notifications.
     */
    void setListener(JmsTransactionListener listener);

    /**
     * @return true if there is a transaction in progress even if the current is failed.
     */
    boolean isInTransaction();

    /**
     * Allows a resource to query the transaction context to determine if it has pending
     * work in the current transaction.
     *
     * Callers should use caution with this method as it is only a view into the current
     * state without blocking ongoing transaction operations.  The best use of this method is
     * in the validation method of a JmsTransactionSynchronization to determine if the
     * synchronization needs to be added based on whether to requesting resource has any
     * pending operations.
     *
     * @param resouceId
     *      The JmsResourceId of the resource making this query.
     *
     * @return true if the resource has pending work in the current transaction.
     */
    boolean isActiveInThisContext(JmsResourceId resouceId);

    /**
     * Signals that the connection that was previously established has been lost and the
     * listener should alter its state to reflect the fact that there is no active connection.
     */
    void onConnectionInterrupted();

    /**
     * Called when the connection to the remote peer has been lost and then a new
     * connection established.  The context should perform any necessary processing
     * recover and reset its internal state.
     *
     * @param provider
     *      A reference to the provider that manages the new connection.
     *
     * @throws Exception if an error occurs while rebuilding against the new provider.
     */
    void onConnectionRecovery(Provider provider) throws Exception;

}
