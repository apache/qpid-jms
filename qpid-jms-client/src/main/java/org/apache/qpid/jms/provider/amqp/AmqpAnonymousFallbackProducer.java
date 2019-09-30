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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.NoOpAsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.jms.provider.amqp.builders.AmqpProducerBuilder;
import org.apache.qpid.jms.provider.exceptions.ProviderIllegalStateException;
import org.apache.qpid.jms.util.IdGenerator;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the case of anonymous JMS MessageProducers.
 *
 * In order to simulate the anonymous producer we must create a sender for each message
 * send attempt and close it following a successful send.
 */
public class AmqpAnonymousFallbackProducer extends AmqpProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAnonymousFallbackProducer.class);
    private static final IdGenerator producerIdGenerator = new IdGenerator();

    private final AmqpConnection connection;
    private final Map<JmsDestination, AmqpFallbackProducer> producerCache = new LinkedHashMap<>(1, 0.75f, true);
    private final String producerIdKey = producerIdGenerator.generateId();
    private long producerIdCount;
    private final ScheduledFuture<?> cacheProducerTimeoutTask;

    /**
     * Creates the Anonymous Producer object.
     *
     * @param session
     *        the session that owns this producer
     * @param info
     *        the JmsProducerInfo for this producer.
     */
    public AmqpAnonymousFallbackProducer(AmqpSession session, JmsProducerInfo info) {
        super(session, info);

        this.connection = session.getConnection();

        final long sweeperInterval = connection.getAnonymousProducerCacheTimeout();
        if (sweeperInterval > 0 && connection.getAnonymousProducerCacheSize() > 0) {
            LOG.trace("Cached Producer timeout monitoring enabled: interval = {}ms", sweeperInterval);
            cacheProducerTimeoutTask = connection.scheduleWithFixedDelay(new CachedProducerSweeper(), sweeperInterval);
        } else {
            LOG.trace("No Cached Producer timeout monitoring enabled based on configuration.");
            cacheProducerTimeoutTask = null;
        }
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        LOG.trace("Started send chain for anonymous producer: {}", getProducerId());
        AmqpFallbackProducer producer = producerCache.get(envelope.getDestination());

        if (producer != null && !producer.isAwaitingClose()) {
            producer.send(envelope, request);
        } else {
            handleSendWhenCachedProducerNotAvailable(envelope, request);
        }
    }

    private void handleSendWhenCachedProducerNotAvailable(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        AmqpFallbackProducer producer = producerCache.get(envelope.getDestination());

        if (producer != null && producer.isAwaitingClose()) {
            // Producer timed out, or was closed due to send failure wait for close to finish then try
            // to open a new link and send this new message.  This prevents the cache from carrying more
            // than one producer on the same address (destination).
            producer.close(new SendPendingCloseRequest(producer, envelope, request));
        } else if (producerCache.size() < connection.getAnonymousProducerCacheSize()) {
            startSendWithNewProducer(envelope, request);
        } else {
            startSendAfterOldestProducerEvicted(envelope, request);
        }
    }

    private void startSendAfterOldestProducerEvicted(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        // No producer in cache yet, or connection limit is zero so we always fall into trying to close oldest if one is open.
        if (producerCache.isEmpty()) {
            startSendWithNewProducer(envelope, request);
        } else {
            // Least recently used producer which will be closed to make room for the new one.
            AmqpFallbackProducer producer = producerCache.values().iterator().next();
            LOG.trace("Next send will commence after producer: {} has been closed", producer);
            producer.close(new SendPendingCloseRequest(producer, envelope, request));
        }
    }

    private void startSendWithNewProducer(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        LOG.trace("Next send will commence after producer for destination {} been opened", envelope.getDestination());

        // Manufactured Producer state for a fixed producer that sends to the target of this specific envelope.
        JmsProducerInfo info = new JmsProducerInfo(getNextProducerId());
        info.setDestination(envelope.getDestination());
        info.setPresettle(this.getResourceInfo().isPresettle());

        // We open a Fixed Producer instance with the target destination.  Once it opens
        // it will trigger the open event which will in turn trigger the send event.
        AmqpProducerBuilder builder = new AmqpFallbackProducerBuilder(session, info);
        builder.buildResource(new FallbackProducerOpenRequest(request, builder, envelope));

        getParent().getProvider().pumpToProtonTransport(request);
    }

    @Override
    public void close(AsyncResult request) {
        if (cacheProducerTimeoutTask != null) {
            cacheProducerTimeoutTask.cancel(false);
        }

        if (producerCache.isEmpty()) {
            request.onSuccess();
        } else {
            AmqpAnonymousFallbackProducerCloseRequest aggregate =
                new AmqpAnonymousFallbackProducerCloseRequest(request, producerCache.size());

            LOG.trace("Anonymous Fallback Producer close will wait for close on {} cached producers", producerCache.size());
            final List<AmqpFallbackProducer> pending = new ArrayList<>(producerCache.values());
            for (AmqpFallbackProducer producer : pending) {
                producer.close(aggregate);
            }

            producerCache.clear();
        }
    }

    @Override
    public boolean isAnonymous() {
        return true;
    }

    @Override
    public EndpointState getLocalState() {
        return EndpointState.ACTIVE;
    }

    @Override
    public EndpointState getRemoteState() {
        return EndpointState.ACTIVE;
    }

    private JmsProducerId getNextProducerId() {
        return new JmsProducerId(producerIdKey, -1, producerIdCount++);
    }

    //----- AsyncResult instances used to manage the life-cycle of the fallback producers

    /*
     * AsyncResult instance that waits for the successful open of a fixed producer instance that
     * will be used for sends and possibly cached for later reuse.  The handler ensures that proper
     * cleanup occurs if the producer couldn't be opened or if the initial send attempt fails.
     */
    private final class FallbackProducerOpenRequest extends WrappedAsyncResult {

        private final JmsOutboundMessageDispatch envelope;
        private final AmqpProducerBuilder producerBuilder;

        public FallbackProducerOpenRequest(AsyncResult sendResult, AmqpProducerBuilder producerBuilder, JmsOutboundMessageDispatch envelope) {
            super(sendResult);

            this.envelope = envelope;
            this.producerBuilder = producerBuilder;
        }

        @Override
        public void onSuccess() {
            LOG.trace("Open phase of anonymous send complete: {} ", getProducerId());

            AmqpFallbackProducer producer = (AmqpFallbackProducer) producerBuilder.getResource();
            // The fixed producer opened so we start tracking it and once send returns indicating that
            // it handled the request (not necessarily sent it but knows it exists) then we start the
            // close clock and if not reused again this producer will eventually time itself out of
            // existence.
            producerCache.put(envelope.getDestination(), producer);

            boolean closeNow = connection.getAnonymousProducerCacheSize() <= 0;

            try {
                producer.send(envelope, getWrappedRequest());
            } catch (ProviderException e) {
                closeNow = true;  // close on send fail to speed up processing of next send or ultimate close on error.
                super.onFailure(e);
            }

            // No cache configured so close the only entry in the map but don't remove it until it
            // has reported itself closed so that next send waits for that to happen preventing
            // runaway link openings.  (Can also trigger if send failed immediately).
            if (closeNow) {
                LOG.trace("Immediate close of fallback producer {} after send triggered.", producer);
                producer.close(new CloseRequest(producer));
            }
        }

        @Override
        public void onFailure(ProviderException result) {
            LOG.debug("Anonymous fallback Send failed because open of new fixed producer failed: {}", getProducerId());

            // Producer open failed so it was never in the cache, just close it now to ensure
            // that everything is cleaned up internally.  The originating send will be failed
            // by failing this AsyncResult as it wraps the original request.
            producerBuilder.getResource().close(NoOpAsyncResult.INSTANCE);
            super.onFailure(result);
        }
    }

    /*
     * Close request handler for the individual anonymous fallback producer instances in the cache
     * that will ensure the entry in the cache is cleared when the close completes.  If the close
     * fails the handler ensures that the provider listener gets a shot of reporting it.
     */
    private final class CloseRequest implements AsyncResult {

        private final AmqpFallbackProducer producer;

        public CloseRequest(AmqpFallbackProducer producer) {
            this.producer = producer;
        }

        @Override
        public void onFailure(ProviderException result) {
            LOG.trace("Close of anonymous producer {} failed: {}", producer, result);
            producerCache.remove(producer.getResourceInfo().getDestination());
            producer.getParent().getProvider().fireProviderException(result);
        }

        @Override
        public void onSuccess() {
            LOG.trace("Close of anonymous producer {} complete", producer);
            producerCache.remove(producer.getResourceInfo().getDestination());
        }

        @Override
        public boolean isComplete() {
            return producer.isClosed();
        }
    }

    /*
     * Close handler for the full anonymous fallback producer instance which waits for all
     * the cached fallback producers to close before singling completion to the caller.  The
     * signal will be success if all closed successfully and failure if any one of them failed.
     */
    private final class AmqpAnonymousFallbackProducerCloseRequest extends WrappedAsyncResult {

        private int pendingCloseRequests;
        private ProviderException firstFailure;

        public AmqpAnonymousFallbackProducerCloseRequest(AsyncResult request, int pendingCloseRequets) {
            super(request);

            this.pendingCloseRequests = pendingCloseRequets;
        }

        @Override
        public void onFailure(ProviderException result) {
            LOG.trace("Close of one anonymous producer done (with error), remaining: {}", pendingCloseRequests);
            pendingCloseRequests--;
            firstFailure = firstFailure == null ? result : firstFailure;
            if (pendingCloseRequests == 0) {
                super.onFailure(firstFailure);
            }
        }

        @Override
        public void onSuccess() {
            LOG.trace("Close of one anonymous producer done, remaining: {}", pendingCloseRequests);
            pendingCloseRequests--;
            if (pendingCloseRequests == 0) {
                if (firstFailure == null) {
                    super.onSuccess();
                } else {
                    super.onFailure(firstFailure);
                }
            }
        }

        @Override
        public boolean isComplete() {
            return pendingCloseRequests == 0;
        }
    }

    /*
     * Request that will initiate a new send operation as soon as the pending close of an already
     * cached fallback producer completes.  The send happens regardless of the outcome of the fallback
     * close as it is unknown why that failed.  The send could then fail but that would allow for normal
     * send failure processing to kick in.
     */
    private final class SendPendingCloseRequest implements AsyncResult {

        private final JmsOutboundMessageDispatch envelope;
        private final AsyncResult sendRequest;
        private final AmqpFallbackProducer pendingCloseProducer;

        public SendPendingCloseRequest(AmqpFallbackProducer producer, JmsOutboundMessageDispatch envelope, AsyncResult sendRequest) {
            this.envelope = envelope;
            this.sendRequest = sendRequest;
            this.pendingCloseProducer = producer;
        }

        @Override
        public void onFailure(ProviderException result) {
            LOG.trace("Close of anonymous producer {} failed: {}", pendingCloseProducer, result);
            producerCache.remove(pendingCloseProducer.getResourceInfo().getDestination());
            pendingCloseProducer.getParent().getProvider().fireProviderException(result);

            try {
                send(envelope, sendRequest);
            } catch (ProviderException ex) {
                sendRequest.onFailure(ex);
            }
        }

        @Override
        public void onSuccess() {
            LOG.trace("Close of anonymous producer {} complete", pendingCloseProducer);
            producerCache.remove(pendingCloseProducer.getResourceInfo().getDestination());

            try {
                send(envelope, sendRequest);
            } catch (ProviderException ex) {
                sendRequest.onFailure(ex);
            }
        }

        @Override
        public boolean isComplete() {
            return pendingCloseProducer.getRemoteState() == EndpointState.CLOSED;
        }
    }

    /*
     * Timeout task used to close any anonymous fallback producer instances that have been
     * inactive for longer than the configured timeout.
     */
    private final class CachedProducerSweeper implements Runnable {

        @Override
        public void run() {
            final List<AmqpFallbackProducer> pending = new ArrayList<>(producerCache.values());
            for (AmqpFallbackProducer producer : pending) {
                if (producer.isExpired()) {
                    LOG.trace("Cached Producer {} has timed out, initiating close", producer);
                    producer.close(new CloseRequest(producer));
                }
            }
        }
    }

    /*
     * Extended fixed destination producer that provides insight into events like remote close which
     * should evict producers from the cache.
     */
    private final class AmqpFallbackProducer extends AmqpFixedProducer {

        private int lastExpiryCheckValue;
        private int activityCounter;

        public AmqpFallbackProducer(AmqpSession session, JmsProducerInfo info, Sender sender, int maxInactiveTime) {
            super(session, info, sender);
        }

        @Override
        public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
            activityCounter++;
            super.send(envelope, request);
        }

        @Override
        public void close(AsyncResult request) {
            if (isAwaitingClose()) {
                // A pending close either from failed send or from expired producer can be superseded
                // by a new send on close request without issue as it will handle any close failures
                // otherwise something terribly wrong has happened.  Similarly it can be replaced with
                // the close request aggregate that is used when the main producer is closed.
                if (closeRequest instanceof CloseRequest) {
                    this.closeRequest = request;
                } else {
                    LOG.error("Close called on producer that already has a pending send on close request: " , this);
                    request.onFailure(new ProviderIllegalStateException(
                            "Illegal send on close call encountered in anonymous fallback producer"));
                }
            } else {
                super.close(request);
            }

            // Ensure that in all cases the close attempt is pushed to the wire immediately.
            getParent().getProvider().pumpToProtonTransport();
        }

        public boolean isExpired() {
            // When awaiting close the producer shouldn't be treated as expired as we already closed it
            // and when it eventually closes it will be removed from the cache and any pending work will
            // be triggered.
            if (!isAwaitingClose() && activityCounter == lastExpiryCheckValue) {
                return true;
            } else {
                lastExpiryCheckValue = activityCounter;
                return false;
            }
        }

        @Override
        public void processDeliveryUpdates(AmqpProvider provider, Delivery delivery) throws ProviderException {
            activityCounter++; // Sender is receiving delivery updates (outcomes) so still active.
            super.processDeliveryUpdates(provider, delivery);
        }

        @Override
        public void processFlowUpdates(AmqpProvider provider) throws ProviderException {
            activityCounter++; // Sender just got a flow so allow for blocked sends to reactivate.
            super.processFlowUpdates(provider);
        }

        @Override
        public void processRemoteClose(AmqpProvider provider) throws ProviderException {
            // When not already closing the remote close will clear a cache slot for use by
            // another send to some other producer or to this same producer which will need to
            // open a new link as the remote has forcibly closed this one.  If awaiting close
            // then we initiated it an as such we will respond to it from the AsyncResult that
            // was passed to the close method and clean out the cache slot once the state change
            // has been processed.
            if (!this.isAwaitingClose()) {
                producerCache.remove(this.getResourceInfo().getDestination());
            }

            super.processRemoteClose(provider);
        }
    }

    /*
     * Builder of the internal producer implementation type used when creating new fixed producers.
     */
    private final class AmqpFallbackProducerBuilder extends AmqpProducerBuilder {

        public AmqpFallbackProducerBuilder(AmqpSession parent, JmsProducerInfo resourceInfo) {
            super(parent, resourceInfo);
        }

        @Override
        protected AmqpProducer createResource(AmqpSession parent, JmsProducerInfo resourceInfo, Sender endpoint) {
            return new AmqpFallbackProducer(getParent(), getResourceInfo(), endpoint, connection.getAnonymousProducerCacheTimeout());
        }
    }
}
