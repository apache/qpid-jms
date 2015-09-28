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
package org.apache.qpid.jms.provider.amqp.builders;

import java.io.IOException;

import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.amqp.AmqpEventSink;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.jms.provider.amqp.AmqpResource;
import org.apache.qpid.jms.provider.amqp.AmqpResourceParent;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.proton.engine.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for all AmqpResource builders.
 */
public abstract class AmqpResourceBuilder<TARGET extends AmqpResource, PARENT extends AmqpResourceParent, INFO extends JmsResource, ENDPOINT extends Endpoint> implements AmqpEventSink {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpResourceBuilder.class);

    protected AsyncResult request;
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
    public void buildResource(AsyncResult request) {
        this.request = request;

        // Create the local end of the manage resource.
        endpoint = createEndpoint(resourceInfo);
        endpoint.setContext(this);
        endpoint.open();

        // Create the resource object now
        resource = createResource(parent, resourceInfo, endpoint);
    }

    //----- Event handlers ---------------------------------------------------//

    @Override
    public void processRemoteOpen(AmqpProvider provider) throws IOException {
        handleOpened(provider);
    }

    @Override
    public void processRemoteClose(AmqpProvider provider) throws IOException {
        handleClosed(provider);
    }

    @Override
    public void processRemoteDetach(AmqpProvider provider) throws IOException {
        // No implementation needed here for this event.
    }

    @Override
    public void processDeliveryUpdates(AmqpProvider provider) throws IOException {
        // No implementation needed here for this event.
    }

    @Override
    public void processFlowUpdates(AmqpProvider provider) throws IOException {
        // No implementation needed here for this event.
    }

    //----- Standard open and close handlers ---------------------------------//

    protected void handleOpened(AmqpProvider provider) throws IOException {

        if (isClosePending()) {
            return;
        }

        if (isOpenedEndpointValid()) {
            afterOpened();

            getEndpoint().setContext(resource);
            getParent().addChildResource(resource);
            getRequest().onSuccess();
        } else {
            getEndpoint().close();
            getEndpoint().free();
            getEndpoint().setContext(null);

            // TODO: Perhaps the validate method should thrown an exception so that we
            // can return a specific error message to the create initiator.
            getRequest().onFailure(new IOException("Failed to open requested endpoint"));
        }
    }

    protected void handleClosed(AmqpProvider provider) throws IOException {
        // If the resource being built is closed during the creation process
        // then this is always an error.

        Exception openError;
        if (hasRemoteError()) {
            openError = AmqpSupport.convertToException(getEndpoint().getRemoteCondition());
        } else {
            openError = getOpenAbortException();
        }

        LOG.warn("Open of resource:({}) failed: {}", resourceInfo, openError.getMessage());

        // This resource is now terminated.
        getEndpoint().close();
        getEndpoint().free();
        getEndpoint().setContext(null);

        getRequest().onFailure(openError);
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
     * is deemed to have been completed and success is signalled.
     */
    protected void afterOpened() {
        // Nothing to do here.
    }

    protected boolean hasRemoteError() {
        return getEndpoint().getRemoteCondition().getCondition() != null;
    }

    /**
     * When aborting the open operation, and there isn't an error condition,
     * provided by the peer, the returned exception will be used instead.
     * A subclass may override this method to provide alternative behavior.
     */
    protected Exception getOpenAbortException() {
        return new IOException("Open failed unexpectedly.");
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
