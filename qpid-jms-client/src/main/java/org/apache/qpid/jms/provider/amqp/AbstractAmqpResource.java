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

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Endpoint;
import org.apache.qpid.proton.engine.EndpointState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base for all AmqpResource implementations to extend.
 *
 * This abstract class wraps up the basic state management bits so that the concrete
 * object don't have to reproduce it.  Provides hooks for the subclasses to initialize
 * and shutdown.
 */
public abstract class AbstractAmqpResource<R extends JmsResource, E extends Endpoint> implements AmqpResource {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractAmqpResource.class);

    protected AsyncResult openRequest;
    protected AsyncResult closeRequest;

    protected E endpoint;
    protected R info;

    /**
     * Creates a new AbstractAmqpResource instance with the JmsResource provided, and
     * sets the Endpoint to null.
     *
     * @param info
     *        The JmsResource instance that this AmqpResource is managing.
     */
    public AbstractAmqpResource(R info) {
        this(info, null);
    }

    /**
     * Creates a new AbstractAmqpResource instance with the JmsResource provided, and
     * sets the Endpoint to the given value.
     *
     * @param info
     *        The JmsResource instance that this AmqpResource is managing.
     * @param endpoint
     *        The Proton Endpoint instance that this object maps to.
     */
    public AbstractAmqpResource(R info, E endpoint) {
        this.info = info;
        this.endpoint = endpoint;
    }

    @Override
    public void open(AsyncResult request) {
        this.openRequest = request;
        doOpen();
        this.endpoint.setContext(this);
        this.endpoint.open();
    }

    @Override
    public boolean isOpen() {
        return this.endpoint.getRemoteState() == EndpointState.ACTIVE;
    }

    @Override
    public boolean isAwaitingOpen() {
        return this.openRequest != null;
    }

    @Override
    public void opened() {
        if (this.openRequest != null) {
            this.openRequest.onSuccess();
            this.openRequest = null;
        }
    }

    @Override
    public void close(AsyncResult request) {
        // If already closed signal success or else the caller might never get notified.
        if (endpoint.getLocalState() == EndpointState.CLOSED) {
            request.onSuccess();
            return;
        }

        this.closeRequest = request;
        doClose();
        this.endpoint.close();
    }

    @Override
    public boolean isClosed() {
        return this.endpoint.getLocalState() == EndpointState.CLOSED;
    }

    @Override
    public boolean isAwaitingClose() {
        return this.closeRequest != null;
    }

    @Override
    public void closed() {
        if (this.closeRequest != null) {
            this.closeRequest.onSuccess();
            this.closeRequest = null;
        }

        this.endpoint.close();
        this.endpoint.free();
    }

    @Override
    public void failed() {
        failed(new JMSException("Remote request failed."));
    }

    @Override
    public void failed(Exception cause) {
        if (openRequest != null) {
            openRequest.onFailure(cause);
            openRequest = null;
        }

        if (closeRequest != null) {
            closeRequest.onFailure(cause);
            closeRequest = null;
        }
    }

    public E getEndpoint() {
        return this.endpoint;
    }

    public R getJmsResource() {
        return this.info;
    }

    public EndpointState getLocalState() {
        if (endpoint == null) {
            return EndpointState.UNINITIALIZED;
        }
        return this.endpoint.getLocalState();
    }

    public EndpointState getRemoteState() {
        if (endpoint == null) {
            return EndpointState.UNINITIALIZED;
        }
        return this.endpoint.getRemoteState();
    }

    @Override
    public Exception getRemoteError() {
        String message = getRemoteErrorMessage();
        Exception remoteError = null;
        Symbol error = endpoint.getRemoteCondition().getCondition();
        if (error.equals(AmqpError.UNAUTHORIZED_ACCESS)) {
            remoteError = new JMSSecurityException(message);
        } else {
            remoteError = new JMSException(message);
        }

        return remoteError;
    }

    @Override
    public String getRemoteErrorMessage() {
        String message = "Received unkown error from remote peer";
        if (endpoint.getRemoteCondition() != null) {
            ErrorCondition error = endpoint.getRemoteCondition();
            if (error.getDescription() != null && !error.getDescription().isEmpty()) {
                message = error.getDescription();
            }
        }

        return message;
    }

    @Override
    public void processStateChange() throws IOException {
        EndpointState remoteState = endpoint.getRemoteState();

        if (remoteState == EndpointState.ACTIVE) {
            if (isAwaitingOpen()) {
                LOG.debug("{} is now open: ", this);
                opened();
            }

            // Should not receive an ACTIVE event if not awaiting the open state.
        } else if (remoteState == EndpointState.CLOSED) {
            if (isAwaitingClose()) {
                LOG.debug("{} is now closed: ", this);
                closed();
            } else if (isAwaitingOpen()) {
                // Error on Open, create exception and signal failure.
                LOG.warn("Open of {} failed: ", this);
                Exception remoteError = this.getRemoteError();
                failed(remoteError);
            } else {
                // TODO - Handle remote asynchronous close.
                LOG.warn("{} was closed remotely.", this);
            }
        }
    }

    @Override
    public void processDeliveryUpdates() throws IOException {
    }

    @Override
    public void processFlowUpdates() throws IOException {
    }

    protected abstract void doOpen();

    protected abstract void doClose();

}
