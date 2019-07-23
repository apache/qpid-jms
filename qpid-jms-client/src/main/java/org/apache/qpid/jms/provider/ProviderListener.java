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
package org.apache.qpid.jms.provider;

import java.net.URI;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsResource;

/**
 * Events interface used to update the listener with changes in provider state.
 */
public interface ProviderListener {

    /**
     * Called when a new Message has arrived for a registered consumer.
     *
     * @param envelope
     *        The dispatch object containing the message and delivery information.
     */
    void onInboundMessage(JmsInboundMessageDispatch envelope);

    /**
     * Called when an outbound message dispatch that requested a completion callback
     * has reached a state where the send can be considered successful based on the QoS
     * level associated of the outbound message.
     *
     * @param envelope
     *      the original outbound message dispatch that is now complete.
     */
    void onCompletedMessageSend(JmsOutboundMessageDispatch envelope);

    /**
     * Called when an outbound message dispatch that requested a completion callback
     * has reached a state where the send can be considered failed.
     *
     * @param envelope
     *      the original outbound message dispatch that should be treated as a failed send.
     * @param cause
     *      the exception that describes the cause of the failed send.
     */
    void onFailedMessageSend(JmsOutboundMessageDispatch envelope, ProviderException cause);

    /**
     * Called from a fault tolerant Provider instance to signal that the underlying
     * connection to the Broker has been lost.  The Provider will attempt to reconnect
     * following this event unless closed.
     *
     * It is considered a programming error to allow any exceptions to be thrown from
     * this notification method.
     *
     * @param remoteURI
     *        The URI of the Broker whose connection was lost.
     */
    void onConnectionInterrupted(URI remoteURI);

    /**
     * Called to indicate that a connection to the Broker has been reestablished and
     * that notified listener should start to recover it's state.  The provider will
     * not transition to the recovered state until the listener notifies the provider
     * that recovery is complete.
     *
     * @param provider
     *        The new Provider instance that will become active after the state
     *        has been recovered.
     *
     * @throws Exception if an error occurs during recovery attempt, this will fail
     *         the Provider that's being used for recovery.
     */
    void onConnectionRecovery(Provider provider) throws Exception;

    /**
     * Called to indicate that a connection to the Broker has been reestablished and
     * that all recovery operations have succeeded and the connection will now be
     * transitioned to a recovered state.  This method gives the listener a chance
     * so send any necessary post recovery commands such as consumer start or message
     * pull for a zero prefetch consumer etc.
     *
     * @param provider
     *        The new Provider instance that will become active after the state
     *        has been recovered.
     *
     * @throws Exception if an error occurs during recovery attempt, this will fail
     *         the Provider that's being used for recovery.
     */
    void onConnectionRecovered(Provider provider) throws Exception;

    /**
     * Called to signal that all recovery operations are now complete and the Provider
     * is again in a normal connected state.
     *
     * It is considered a programming error to allow any exceptions to be thrown from
     * this notification method.
     *
     * @param remoteURI
     *        The URI of the Broker that the client has now connected to.
     */
    void onConnectionRestored(URI remoteURI);

    /**
     * Called to indicate that the underlying connection to the Broker has been established
     * for the first time.  For a fault tolerant provider this event should only ever be
     * triggered once with the interruption and recovery events following on for future
     *
     * @param remoteURI
     *        The URI of the Broker that the client has now connected to.
     */
    void onConnectionEstablished(URI remoteURI);

    /**
     * Called to indicate that the underlying connection to the Broker has been lost and
     * the Provider will not perform any reconnect.  Following this call the provider is
     * in a failed state and further calls to it will throw an Exception.
     *
     * @param ex
     *        The exception that indicates the cause of this Provider failure.
     */
    void onConnectionFailure(ProviderException ex);

    /**
     * Called to indicate that a currently active resource has been closed
     * due to some error condition, management request or some other action.
     * This can either be initiated remotely or locally depending on the
     * condition that triggers the close.
     *
     * @param resource
     *        the JmsResource instance that has been closed.
     * @param cause
     *        optional exception object that indicates the cause of the close.
     */
    void onResourceClosed(JmsResource resource, ProviderException cause);

    /**
     * Called to indicate that a some client operation caused or received an
     * error that is not considered fatal at the provider level.
     *
     * @param cause
     *        the exception object that is being reported to the listener.
     */
    void onProviderException(ProviderException cause);

}
