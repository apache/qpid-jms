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
package org.apache.qpid.jms.provider.amqp.builders;

import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.AmqpEventSink;
import org.apache.qpid.jms.provider.amqp.AmqpExceptionBuilder;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.jms.provider.amqp.AmqpResource;
import org.apache.qpid.jms.provider.amqp.AmqpResourceParent;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Endpoint;
import org.apache.qpid.proton.engine.EndpointState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for all AmqpResource builders.
 *
 * @param <TARGET> The Type of resource that will be created.
 * @param <PARENT> The Type of this resource's parent.
 * @param <INFO> The Type of JmsResource used to describe the target resource.
 * @param <ENDPOINT> The AMQP Endpoint that the target resource encapsulates.
 */
public abstract class AmqpResourceBuilder<TARGET extends AmqpResource, PARENT extends AmqpResourceParent, INFO extends JmsResource, ENDPOINT extends Endpoint> implements AmqpEventSink, AmqpExceptionBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpResourceBuilder.class);

    protected AsyncResult request;
    protected ScheduledFuture<?> requestTimeoutTask;
    protected TARGET resource;
    protected ENDPOINT endpoint;
    protected final PARENT parent;
    protected final INFO resourceInfo;

    public AmqpResourceBuilder(PARENT parent, INFO resourceInfo) {
        this.parent = parent;
        this.resourceInfo = resourceInfo;
    }

    /**
     * Called to initiate the process of building the resource type that is
     * managed by this builder.  The resource is created and the open process
     * occurs asynchronously.  If the resource is successfully opened it will
     * added to its parent resource for use.
     *
     * @param request
     *      The request that initiated the resource creation.
     */
    public void buildResource(final AsyncResult request) {
        this.request = request;

        // Create the local end of the manage resource.
        endpoint = createEndpoint(resourceInfo);
        endpoint.setContext(this);
        endpoint.open();

        // Create the resource object now
        resource = createResource(parent, resourceInfo, endpoint);

        AmqpProvider provider = parent.getProvider();

        if (getRequestTimeout() > JmsConnectionInfo.INFINITE) {

            // Attempt to schedule a cancellation of the pending open request, can return
            // null if there is no configured request timeout.
            requestTimeoutTask = provider.scheduleRequestTimeout(new AsyncResult() {

                @Override
                public void onSuccess() {
                    // Nothing to do here.
                }

                @Override
                public void onFailure(ProviderException result) {
                    handleClosed(provider, result);
                }

                @Override
                public boolean isComplete() {
                    return request.isComplete();
                }

            }, getRequestTimeout(), this);
        }

        // Check it wasn't already opened, if it is then handle it
        if (endpoint.getRemoteState() != EndpointState.UNINITIALIZED) {
            provider.scheduleExecuteAndPump(new Runnable() {
                @Override
                public void run() {
                    handleOpened(provider);
                }
            });
        }
    }

    //----- Event handlers ---------------------------------------------------//

    @Override
    public void processRemoteOpen(AmqpProvider provider) throws ProviderException {
        handleOpened(provider);
    }

    @Override
    public void processRemoteClose(AmqpProvider provider) throws ProviderException {
        handleClosed(provider, null);
    }

    @Override
    public void processRemoteDetach(AmqpProvider provider) throws ProviderException {
        // No implementation needed here for this event.
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider, Delivery delivery) throws ProviderException {
        // No implementation needed here for this event.
    }

    @Override
    public void processFlowUpdates(AmqpProvider provider) throws ProviderException {
        // No implementation needed here for this event.
    }

    //----- Standard open and close handlers ---------------------------------//

    protected final void handleOpened(AmqpProvider provider) {

        // perform any post open processing prior to opened state inspection.
        afterOpened();

        if (isClosePending()) {
            return;
        }

        if (requestTimeoutTask != null) {
            requestTimeoutTask.cancel(false);
        }

        if (isOpenedEndpointValid()) {
            resourceInfo.setState(ResourceState.OPEN);
            getEndpoint().setContext(resource);
            getParent().addChildResource(resource);
            getRequest().onSuccess();
        } else {
            // TODO: Perhaps the validate method should thrown an exception so that we
            // can return a specific error message to the create initiator.
            handleClosed(provider, new ProviderException("Failed to open requested endpoint"));
        }
    }

    protected final void handleClosed(AmqpProvider provider, ProviderException cause) {
        // If the resource being built is closed during the creation process
        // then this is always an error.

        resourceInfo.setState(ResourceState.REMOTELY_CLOSED);

        // Perform any post processing relating to closure during creation attempt
        afterClosed(getResource(), getResourceInfo());

        ProviderException openError;
        if (hasRemoteError()) {
            openError = getOpenAbortExceptionFromRemote();
        } else if (cause != null) {
            openError = cause;
        } else {
            openError = getDefaultOpenAbortException();
        }

        if (requestTimeoutTask != null) {
            requestTimeoutTask.cancel(false);
        }

        LOG.warn("Open of resource:({}) failed: {}", resourceInfo, openError.getMessage());

        // This resource is now terminated.
        getEndpoint().close();
        getEndpoint().free();
        getEndpoint().setContext(null);

        getRequest().onFailure(openError);
    }

    @Override
    public ProviderException createException() {
        return new ProviderOperationTimedOutException("Request to open resource " + getResource() + " timed out");
    }

    //----- Implementation methods used to customize the build process -------//

    /**
     * Given the resource information provided create and configure the local endpoint
     * whose open phase is managed by this builder.
     *
     * @return a new endpoint to be managed.
     */
    protected abstract ENDPOINT createEndpoint(INFO resourceInfo);

    /**
     * Create the managed resource instance.
     *
     * @param parent
     *      The parent of the newly created resource.
     * @param resourceInfo
     *      The resource information used to configure the resource.
     * @param endpoint
     *      The local endpoint for the managed resource to wrap.
     *
     * @return the resource instance who open life-cycle is managed by this builder.
     */
    protected abstract TARGET createResource(PARENT parent, INFO resourceInfo, ENDPOINT endpoint);

    /**
     * If the resource was opened but its current state indicates a close is pending
     * then we do no need to proceed further into the resource creation process.  Each
     * endpoint build must implement this and examine the opened endpoint to determine
     * if a close frame will follow the open.
     *
     * @return true if the resource state indicates it will be immediately closed.
     */
    protected abstract boolean isClosePending();

    /**
     * Following the open of the endpoint implementations of this method should validate
     * that the endpoint properties match what was requested.
     *
     * @return true if the endpoint is valid based on what was requested.
     */
    protected boolean isOpenedEndpointValid() {
        return true;
    }

    /**
     * Called once an endpoint has been opened and validated to give the subclasses a
     * place to perform any follow-on processing or setup steps before the operation
     * is deemed to have been completed and success is signaled.
     */
    protected void afterOpened() {
        // Nothing to do here.
    }

    /**
     * Called if endpoint opening process fails in order to give the subclasses a
     * place to perform any follow-on processing or teardown steps before the operation
     * is deemed to have been completed and failure is signalled.
     *
     * @param resource the resource
     * @param resourceInfo the resourceInfo
     */
    protected void afterClosed(TARGET resource, INFO resourceInfo) {
        // Nothing to do here.
    }

    protected boolean hasRemoteError() {
        return getEndpoint().getRemoteCondition().getCondition() != null;
    }

    /**
     * When aborting the open operation, and there isn't an error condition,
     * provided by the peer, the returned exception will be used instead.
     * A subclass may override this method to provide alternative behavior.
     *
     * @return an Exception to describes the open failure for this resource.
     */
    protected ProviderException getDefaultOpenAbortException() {
        return new ProviderException("Open failed unexpectedly.");
    }

    /**
     * When aborting the open operation, this method will attempt to create an
     * appropriate exception from the remote error condition if one is set and will
     * revert to creating the default variant if not.
     *
     * @return an Exception to describes the open failure for this resource.
     */
    protected ProviderException getOpenAbortExceptionFromRemote() {
        return AmqpSupport.convertToNonFatalException(parent.getProvider(), getEndpoint(), getEndpoint().getRemoteCondition());
    }

    /**
     * Returns the configured time before the open of the resource is considered
     * to have failed.  Subclasses can override this method to provide a value more
     * appropriate to the resource being built.
     *
     * @return the configured timeout before the open of the resource fails.
     */
    protected long getRequestTimeout() {
        return getParent().getProvider().getRequestTimeout();
    }

    //----- Public access methods for the managed resources ------------------//

    public ENDPOINT getEndpoint() {
        return endpoint;
    }

    public AsyncResult getRequest() {
        return request;
    }

    public TARGET getResource() {
        return resource;
    }

    public PARENT getParent() {
        return parent;
    }

    public INFO getResourceInfo() {
        return resourceInfo;
    }
}
