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

import java.net.URI;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;

/**
 * Providers an interface for client's to listener to events related to
 * an JmsConnection.
 */
public interface JmsConnectionListener {

    /**
     * Called when a connection has been successfully established.
     *
     * This method is never called more than once when using a fault tolerant
     * connection, instead the connection will signal interrupted and restored.
     *
     * @param remoteURI
     *        The URI of the Broker this client is now connected to.
     */
    void onConnectionEstablished(URI remoteURI);

    /**
     * Called when an unrecoverable error occurs and the Connection must be closed.
     *
     * @param error
     *        The error that triggered the failure.
     */
    void onConnectionFailure(Throwable error);

    /**
     * Called when the Connection to the remote peer is lost.
     *
     * @param remoteURI
     *        The URI of the Broker previously connected to.
     */
    void onConnectionInterrupted(URI remoteURI);

    /**
     * Called when normal communication has been restored to a remote peer.
     *
     * @param remoteURI
     *        The URI of the Broker that this client is now connected to.
     */
    void onConnectionRestored(URI remoteURI);

    /**
     * Called when a Connection is notified that a new Message has arrived for
     * one of it's currently active subscriptions.
     *
     * @param envelope
     *        The envelope that contains the incoming message and it's delivery information.
     */
    void onInboundMessage(JmsInboundMessageDispatch envelope);

    /**
     * Called when the session is closed due to remote action or local error detection.
     *
     * @param session
     *      The session that was closed and needs to be cleaned up.
     * @param cause
     *      The exception that provides additional context on the closure.
     */
    void onSessionClosed(Session session, Throwable cause);

    /**
     * Called when the MessageConsumer is closed due to remote action or local error detection.
     *
     * @param consumer
     *      The consumer that was closed and needs to be cleaned up.
     * @param cause
     *      The exception that provides additional context on the closure.
     */
    void onConsumerClosed(MessageConsumer consumer, Throwable cause);

    /**
     * Called when the MessageProducer is closed due to remote action or local error detection.
     *
     * @param producer
     *      The producer that was closed and needs to be cleaned up.
     * @param cause
     *      The exception that provides additional context on the closure.
     */
    void onProducerClosed(MessageProducer producer, Throwable cause);

}
