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

import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.proton.engine.Delivery;
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
 *
 * @param <R> The JmsResource type that describe this resource.
 * @param <E> The AMQP Endpoint that this resource encapsulates.
 */
public abstract class AmqpAbstractResource<R extends JmsResource, E extends Endpoint> implements AmqpResource {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAbstractResource.class);

    protected AsyncResult closeRequest;
    protected ScheduledFuture<?> closeTimeoutTask;

    private final E endpoint;
    private final R resourceInfo;
    private final AmqpResourceParent parent;

    /**
     * Creates a new instance with the JmsResource provided, and sets the Endpoint to the given value.
     *
     * @param resourceInfo
     *        The JmsResource instance that this AmqpResource is managing.
     * @param endpoint
     *        The Proton Endpoint instance that this object maps to.
     */
    public AmqpAbstractResource(R resourceInfo, E endpoint) {
        this(resourceInfo, endpoint, null);
    }

    /**
     * Creates a new instance with the JmsResource provided, and sets the Endpoint to the given value.
     *
     * @param resourceInfo
     *        The JmsResource instance that this AmqpResource is managing.
     * @param endpoint
     *        The Proton Endpoint instance that this object maps to.
     * @param parent
     *        The parent of this resource (null if no parent).
     */
    public AmqpAbstractResource(R resourceInfo, E endpoint, AmqpResourceParent parent) {
        this.parent = parent;
        this.endpoint = endpoint;
        this.resourceInfo = resourceInfo;
        this.resourceInfo.getId().setProviderHint(this);
    }

    public void close(AsyncResult request) {
        if (parent != null) {
            parent.removeChildResource(this);
        }

        resourceInfo.setState(ResourceState.CLOSED);

        // If already closed signal success or else the caller might never get notified.
        if (getEndpoint().getLocalState() == EndpointState.CLOSED ||
            getEndpoint().getRemoteState() == EndpointState.CLOSED) {

            // Remote already closed this resource, close locally and free.
            if (getEndpoint().getLocalState() != EndpointState.CLOSED) {
                getEndpoint().close();
                getEndpoint().free();
                getEndpoint().setContext(null);
            }

            request.onSuccess();
            return;
        }

        LOG.trace("{} requesting close on remote.", this);

        closeRequest = request;

        // Use close timeout for all resource closures and fallback to the request
        // timeout if the close timeout was not set
        long closeTimeout = getParent().getProvider().getCloseTimeout();
        if (closeTimeout == JmsConnectionInfo.INFINITE) {
            closeTimeout = getParent().getProvider().getRequestTimeout();
        }

        if (closeTimeout != JmsConnectionInfo.INFINITE) {
            closeTimeoutTask = getParent().getProvider().scheduleRequestTimeout(
                new AsyncResult() {

                    @Override
                    public void onSuccess() {
                        // Not called in this context.
                    }

                    @Override
                    public void onFailure(ProviderException result) {
                        closeResource(getParent().getProvider(), result, false);
                    }

                    @Override
                    public boolean isComplete() {
                        return closeRequest != null ? closeRequest.isComplete() : true;
                    }

                }, closeTimeout, new ProviderOperationTimedOutException("Timed Out Waiting for close response: " + this));
        }

        closeOrDetachEndpoint();
    }

    public void closeResource(AmqpProvider provider, ProviderException cause, boolean remotelyClosed) {
        if (parent != null) {
            parent.removeChildResource(this);
        }

        resourceInfo.setState(ResourceState.CLOSED);

        if (getEndpoint() != null) {
            // TODO: if this is a producer/consumer link then we may only be detached,
            // rather than fully closed, and should respond appropriately.
            closeOrDetachEndpoint();

            // Process the close before moving on to closing down child resources
            provider.pumpToProtonTransport();

            handleResourceClosure(provider, cause);

            // Now clean up after the close has completed if the close is not initiated
            // from this client, otherwise we need to wait for the remote to respond.
            if (remotelyClosed) {
                getEndpoint().free();
                getEndpoint().setContext(null);
            }
        }

        if (closeTimeoutTask != null) {
            closeTimeoutTask.cancel(true);
            closeTimeoutTask = null;
        }

        if (isAwaitingClose()) {
            LOG.debug("{} is now closed: ", this);
            if (cause == null) {
                closeRequest.onSuccess();
            } else {
                closeRequest.onFailure(cause);
            }
            closeRequest = null;
        } else {
            if (cause != null) {
                if (getResourceInfo() instanceof JmsConnectionInfo) {
                    provider.fireProviderException(cause);
                } else {
                    provider.fireResourceClosed(getResourceInfo(), cause);
                }
            }
        }
    }

    public void handleResourceClosure(AmqpProvider provider, ProviderException error) {
        // Nothing do be done here, subclasses can override as needed.
    }

    /**
     * Perform the close operation on the managed endpoint.  A subclass may
     * override this method to alter the standard close path such as endpoint
     * detach etc.
     */
    protected void closeOrDetachEndpoint() {
        getEndpoint().close();
    }

    //----- Access methods ---------------------------------------------------//

    public E getEndpoint() {
        return endpoint;
    }

    public R getResourceInfo() {
        return resourceInfo;
    }

    public AmqpResourceParent getParent() {
        return parent;
    }

    //----- Endpoint state access methods ------------------------------------//

    public boolean isOpen() {
        return getEndpoint().getRemoteState() == EndpointState.ACTIVE;
    }

    public boolean isClosed() {
        return getEndpoint().getLocalState() == EndpointState.CLOSED;
    }

    public boolean isAwaitingClose() {
        return closeRequest != null;
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

    //----- AmqpResource implementation --------------------------------------//

    @Override
    public final void processRemoteOpen(AmqpProvider provider) throws ProviderException {
        // Open is handled by the resource builder
    }

    @Override
    public void processRemoteDetach(AmqpProvider provider) throws ProviderException {
        processRemoteClose(provider);
    }

    @Override
    public void processRemoteClose(AmqpProvider provider) throws ProviderException {
        getResourceInfo().setState(ResourceState.REMOTELY_CLOSED);

        if (isAwaitingClose()) {
            closeResource(provider, null, true); // Close was expected so ignore any endpoint errors.
        } else {
            // For resources other than the Connection layer a remote close is not fatal, the client
            // can conceivably continue on or opt to close down on its own.
            closeResource(provider, AmqpSupport.convertToNonFatalException(provider, getEndpoint(), getEndpoint().getRemoteCondition()), true);
        }
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider, Delivery delivery) throws ProviderException {
        // Nothing do be done here, subclasses can override as needed.
    }

    @Override
    public void processFlowUpdates(AmqpProvider provider) throws ProviderException {
        // Nothing do be done here, subclasses can override as needed.
    }
}
