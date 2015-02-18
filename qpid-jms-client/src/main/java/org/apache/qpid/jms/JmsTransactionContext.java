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

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;

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
     *
     * @throws JMSException if an error occurs during the send.
     */
    void send(JmsConnection connection, JmsOutboundMessageDispatch envelope) throws JMSException;

    /**
     * Adds the given Transaction synchronization to the current list. The
     * registered synchronization will be notified of various event points
     * in the lifetime of a transaction such as before and after commit or
     * rollback.
     *
     * @param sync
     *        the transaction synchronization to add.
     *
     * @throws JMSException if an error occurs during the send.
     */
    void addSynchronization(JmsTxSynchronization sync) throws JMSException;

    /**
     * @returns if the currently transaction has been marked as being failed.
     */
    boolean isFailed();

    /**
     * Start a transaction if none is currently active.
     *
     * @throws JMSException on internal error occurs.
     */
    void begin() throws JMSException;

    /**
     * Rolls back any work done in this transaction and releases any locks
     * currently held.  If the current transaction is in a failed state this
     * resets that state and prepares the context for a new transaction to be
     * stated via a call to <code>begin</code>.
     *
     * @throws JMSException
     *         if the JMS provider fails to roll back the transaction due to some internal error.
     */
    void rollback() throws JMSException;

    /**
     * Commits all work done in this transaction and releases any locks
     * currently held.  If the transaction is in a failed state this method
     * throws an exception to indicate that the transaction has failed and
     * will be rolled back.
     *
     * @throws JMSException
     *         if the commit fails to roll back the transaction due to some internal error.
     */
    void commit() throws JMSException;

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

    void onConnectionInterrupted();

    /**
     * Called when the connection to the remote peer has been lost and then a new
     * connection established.  The context should perform any necessary processing
     * recover and reset its internal state.
     */
    void onConnectionRecovery(Provider provider) throws Exception;

}
