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

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.provider.AsyncResult;
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
public abstract class AmqpAbstractResource<R extends JmsResource, E extends Endpoint> implements AmqpResource {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAbstractResource.class);

    protected AsyncResult openRequest;
    protected AsyncResult closeRequest;

    private E endpoint;
    protected R resource;

    /**
     * Creates a new instance with the JmsResource provided, and sets the Endpoint to null.
     *
     * @param resource
     *        The JmsResource instance that this AmqpResource is managing.
     */
    public AmqpAbstractResource(R resource) {
        this(resource, null);
    }

    /**
     * Creates a new instance with the JmsResource provided, and sets the Endpoint to the given value.
     *
     * @param resource
     *        The JmsResource instance that this AmqpResource is managing.
     * @param endpoint
     *        The Proton Endpoint instance that this object maps to.
     */
    public AmqpAbstractResource(R resource, E endpoint) {
        this.resource = resource;
        setEndpoint(endpoint);
    }

    @Override
    public void open(AsyncResult request) {
        this.openRequest = request;
        doOpen();
        getEndpoint().setContext(this);
    }

    @Override
    public boolean isOpen() {
        return getEndpoint().getRemoteState() == EndpointState.ACTIVE;
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
        if (getEndpoint().getLocalState() == EndpointState.CLOSED ||
            getEndpoint().getRemoteState() == EndpointState.CLOSED) {

            // Remote already closed this resource, close locally and free.
            if (getEndpoint().getLocalState() != EndpointState.CLOSED) {
                doClose();
                getEndpoint().free();
            }

            request.onSuccess();
            return;
        }

        this.closeRequest = request;
        doClose();
    }

    @Override
    public boolean isClosed() {
        return getEndpoint().getLocalState() == EndpointState.CLOSED;
    }

    @Override
    public boolean isAwaitingClose() {
        return this.closeRequest != null;
    }

    @Override
    public void closed() {
        endpoint.close();
        endpoint.free();

        if (this.closeRequest != null) {
            this.closeRequest.onSuccess();
            this.closeRequest = null;
        }
    }

    @Override
    public void failed(Exception cause) {
        if (openRequest != null) {
            if (endpoint != null) {
                // TODO: if this is a producer/consumer link then we may only be detached,
                // rather than fully closed, and should respond appropriately.
                endpoint.close();
            }
            openRequest.onFailure(cause);
            openRequest = null;
        }

        if (closeRequest != null) {
            closeRequest.onFailure(cause);
            closeRequest = null;
        }
    }

    @Override
    public void remotelyClosed(AmqpProvider provider) {
        Exception error = AmqpSupport.convertToException(getEndpoint().getRemoteCondition());

        if (endpoint != null) {
            // TODO: if this is a producer/consumer link then we may only be detached,
            // rather than fully closed, and should respond appropriately.
            endpoint.close();
        }

        LOG.info("Resource {} was remotely closed", getJmsResource());

        if (getJmsResource() instanceof JmsConnectionInfo) {
            provider.fireProviderException(error);
        } else {
            provider.fireResourceRemotelyClosed(getJmsResource(), error);
        }
    }

    public E getEndpoint() {
        return this.endpoint;
    }

    public void setEndpoint(E endpoint) {
        this.endpoint = endpoint;
    }

    public R getJmsResource() {
        return this.resource;
    }

    public EndpointState getLocalState() {
        if (getEndpoint() == null) {
            return EndpointState.UNINITIALIZED;
        }
        return getEndpoint().getLocalState();
    }

    public EndpointState getRemoteState() {
        if (getEndpoint() == null) {
            return EndpointState.UNINITIALIZED;
        }
        return getEndpoint().getRemoteState();
    }

    protected boolean hasRemoteError() {
        return getEndpoint().getRemoteCondition().getCondition() != null;
    }

    @Override
    public void processRemoteOpen(AmqpProvider provider) throws IOException {
        doOpenCompletion();
    }

    @Override
    public void processRemoteDetach(AmqpProvider provider) throws IOException {
        if (isAwaitingClose()) {
            LOG.debug("{} is now closed: ", this);
            closed();
        } else {
            remotelyClosed(provider);
        }
    }

    @Override
    public void processRemoteClose(AmqpProvider provider) throws IOException {
        if (isAwaitingClose()) {
            LOG.debug("{} is now closed: ", this);
            closed();
        } else if (isAwaitingOpen()) {
            // Error on Open, create exception and signal failure.
            LOG.warn("Open of {} failed: ", this);
            Exception openError;
            if (hasRemoteError()) {
                openError = AmqpSupport.convertToException(getEndpoint().getRemoteCondition());
            } else {
                openError = getOpenAbortException();
            }

            failed(openError);
        } else {
            remotelyClosed(provider);
        }
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider) throws IOException {
    }

    @Override
    public void processFlowUpdates(AmqpProvider provider) throws IOException {
    }

    /**
     * Perform the open operation on the managed endpoint.  A subclass may
     * override this method to provide additional open actions or configuration
     * updates.
     */
    protected void doOpen() {
        getEndpoint().open();
    }

    /**
     * Complete the open operation on the managed endpoint. A subclass may
     * override this method to provide additional verification actions or configuration
     * updates.
     */
    protected void doOpenCompletion() {
        LOG.debug("{} is now open: ", this);
        opened();
    }

    /**
     * When aborting the open operation, and there isn't an error condition,
     * provided by the peer, the returned exception will be used instead.
     * A subclass may override this method to provide alternative behavior.
     */
    protected Exception getOpenAbortException() {
        return new IOException("Open failed unexpectedly.");
    }

    /**
     * Perform the close operation on the managed endpoint.  A subclass may
     * override this method to provide additional close actions or alter the
     * standard close path such as endpoint detach etc.
     */
    protected void doClose() {
        getEndpoint().close();
    }
}
