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

import org.apache.qpid.jms.provider.AsyncResult;

/**
 * AmqpResource specification.
 *
 * All AMQP types should implement this interface to allow for control of state
 * and configuration details.
 */
public interface AmqpResource {

    /**
     * Perform all the work needed to open this resource and store the request
     * until such time as the remote peer indicates the resource has become active.
     *
     * @param request
     *        The initiating request that triggered this open call.
     */
    void open(AsyncResult request);

    /**
     * @return if the resource has moved to the opened state on the remote.
     */
    boolean isOpen();

    /**
     * @return true if the resource is awaiting the remote end to signal opened.
     */
    boolean isAwaitingOpen();

    /**
     * Called to indicate that this resource is now remotely opened.  Once opened a
     * resource can start accepting incoming requests.
     */
    void opened();

    /**
     * Perform all work needed to close this resource and store the request
     * until such time as the remote peer indicates the resource has been closed.
     *
     * @param request
     *        The initiating request that triggered this close call.
     */
    void close(AsyncResult request);

    /**
     * @return if the resource has moved to the closed state on the remote.
     */
    boolean isClosed();

    /**
     * @return true if the resource is awaiting the remote end to signal closed.
     */
    boolean isAwaitingClose();

    /**
     * Called to indicate that this resource is now remotely closed.  Once closed a
     * resource can not accept any incoming requests.
     */
    void closed();

    /**
     * Sets the failed state for this Resource and triggers a failure signal for
     * any pending ProduverRequest.
     */
    void failed();

    /**
     * Sets the failed state for this Resource and triggers a failure signal for
     * any pending ProduverRequest.
     *
     * @param cause
     *        The Exception that triggered the failure.
     */
    void failed(Exception cause);

    /**
     * Called when the Proton Engine signals that the state of the given resource has
     * changed on the remote side.
     *
     * @throws IOException if an error occurs while processing the update.
     */
    void processStateChange() throws IOException;

    /**
     * Called when the Proton Engine signals an Delivery related event has been triggered
     * for the given endpoint.
     *
     * @throws IOException if an error occurs while processing the update.
     */
    void processDeliveryUpdates() throws IOException;

    /**
     * Called when the Proton Engine signals an Flow related event has been triggered
     * for the given endpoint.
     *
     * @throws IOException if an error occurs while processing the update.
     */
    void processFlowUpdates() throws IOException;

    /**
     * @return an Exception derived from the error state of the endpoint's Remote Condition.
     */
    Exception getRemoteError();

    /**
     * @return an Error message derived from the error state of the endpoint's Remote Condition.
     */
    String getRemoteErrorMessage();

}
