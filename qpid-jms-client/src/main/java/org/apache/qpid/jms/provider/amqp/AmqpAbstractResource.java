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

    protected AsyncResult closeRequest;

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

        closeRequest = request;

        closeOrDetachEndpoint();
    }

    public void resourceClosed() {
        getEndpoint().close();
        getEndpoint().free();
        getEndpoint().setContext(null);

        if (this.closeRequest != null) {
            this.closeRequest.onSuccess();
            this.closeRequest = null;
        }
    }

    public void remotelyClosed(AmqpProvider provider) {
        Exception error = AmqpSupport.convertToException(getEndpoint().getRemoteCondition());

        if (parent != null) {
            parent.removeChildResource(this);
        }

        if (endpoint != null) {
            // TODO: if this is a producer/consumer link then we may only be detached,
            // rather than fully closed, and should respond appropriately.
            endpoint.close();
        }

        LOG.info("Resource {} was remotely closed", getResourceInfo());

        if (getResourceInfo() instanceof JmsConnectionInfo) {
            provider.fireProviderException(error);
        } else {
            provider.fireResourceRemotelyClosed(getResourceInfo(), error);
        }
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
        return this.endpoint;
    }

    public R getResourceInfo() {
        return this.resourceInfo;
    }

    //----- Endpoint state access methods ------------------------------------//

    public boolean isOpen() {
        return getEndpoint().getRemoteState() == EndpointState.ACTIVE;
    }

    public boolean isClosed() {
        return getEndpoint().getLocalState() == EndpointState.CLOSED;
    }

    public boolean isAwaitingClose() {
        return this.closeRequest != null;
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
    public final void processRemoteOpen(AmqpProvider provider) throws IOException {
        // Open is handled by the resource builder
    }

    @Override
    public void processRemoteDetach(AmqpProvider provider) throws IOException {
        if (isAwaitingClose()) {
            LOG.debug("{} is now closed: ", this);
            resourceClosed();
        } else {
            remotelyClosed(provider);
        }
    }

    @Override
    public void processRemoteClose(AmqpProvider provider) throws IOException {
        if (isAwaitingClose()) {
            LOG.debug("{} is now closed: ", this);
            resourceClosed();
        } else {
            remotelyClosed(provider);
        }
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider) throws IOException {
        // Nothing do be done here, subclasses can override as needed.
    }

    @Override
    public void processFlowUpdates(AmqpProvider provider) throws IOException {
        // Nothing do be done here, subclasses can override as needed.
    }
}
