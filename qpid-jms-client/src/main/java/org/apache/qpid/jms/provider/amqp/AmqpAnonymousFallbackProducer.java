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

import java.util.Map;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.jms.provider.amqp.builders.AmqpProducerBuilder;
import org.apache.qpid.jms.util.IdGenerator;
import org.apache.qpid.jms.util.LRUCache;
import org.apache.qpid.proton.engine.EndpointState;
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

    private final AnonymousProducerCache producerCache;
    private final String producerIdKey = producerIdGenerator.generateId();
    private long producerIdCount;

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

        if (connection.isAnonymousProducerCache()) {
            producerCache = new AnonymousProducerCache(10);
            producerCache.setMaxCacheSize(connection.getAnonymousProducerCacheSize());
        } else {
            producerCache = null;
        }
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        LOG.trace("Started send chain for anonymous producer: {}", getProducerId());

        // Force sends marked as asynchronous to be sent synchronous so that the temporary
        // producer instance can handle failures and perform necessary completion work on
        // the send.
        envelope.setSendAsync(false);

        AmqpProducer producer = null;
        if (connection.isAnonymousProducerCache()) {
            producer = producerCache.get(envelope.getDestination());
        }

        if (producer == null) {
            // Create a new ProducerInfo for the short lived producer that's created to perform the
            // send to the given AMQP target.
            JmsProducerInfo info = new JmsProducerInfo(getNextProducerId());
            info.setDestination(envelope.getDestination());
            info.setPresettle(this.getResourceInfo().isPresettle());

            // We open a Fixed Producer instance with the target destination.  Once it opens
            // it will trigger the open event which will in turn trigger the send event.
            // If caching is disabled the created producer will be closed immediately after
            // the entire send chain has finished and the delivery has been acknowledged.
            AmqpProducerBuilder builder = new AmqpProducerBuilder(session, info);
            builder.buildResource(new AnonymousSendRequest(request, builder, envelope));

            if (connection.isAnonymousProducerCache()) {
                // Cache it in hopes of not needing to create large numbers of producers.
                producerCache.put(envelope.getDestination(), builder.getResource());
            }

            getParent().getProvider().pumpToProtonTransport(request);
        } else {
            producer.send(envelope, request);
        }
    }

    @Override
    public void close(AsyncResult request) {
        // Trigger an immediate close, the internal producers that are currently in the cache
        // if the cache is enabled.
        if (connection.isAnonymousProducerCache()) {
            for (AmqpProducer producer : producerCache.values()) {
                producer.close(new CloseRequest(producer));
            }
        }

        request.onSuccess();
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

    //----- AsyncResult objects used to complete the sends -------------------//

    private abstract class AnonymousRequest extends WrappedAsyncResult {

        protected final JmsOutboundMessageDispatch envelope;

        public AnonymousRequest(AsyncResult sendResult, JmsOutboundMessageDispatch envelope) {
            super(sendResult);
            this.envelope = envelope;
        }

        /**
         * In all cases of the chain of events that make up the send for an anonymous
         * producer a failure will trigger the original send request to fail.
         */
        @Override
        public void onFailure(ProviderException result) {
            LOG.debug("Send failed during {} step in chain: {}", this.getClass().getName(), getProducerId());
            super.onFailure(result);
        }

        public abstract AmqpProducer getProducer();
    }

    private final class AnonymousSendRequest extends AnonymousRequest {

        private final AmqpProducerBuilder producerBuilder;

        public AnonymousSendRequest(AsyncResult sendResult, AmqpProducerBuilder producerBuilder, JmsOutboundMessageDispatch envelope) {
            super(sendResult, envelope);

            this.producerBuilder = producerBuilder;
        }

        @Override
        public void onSuccess() {
            LOG.trace("Open phase of anonymous send complete: {} ", getProducerId());
            AnonymousSendCompleteRequest send = new AnonymousSendCompleteRequest(this);
            try {
                getProducer().send(envelope, send);
            } catch (ProviderException e) {
                super.onFailure(e);
            }
        }

        @Override
        public AmqpProducer getProducer() {
            return producerBuilder.getResource();
        }
    }

    private final class AnonymousSendCompleteRequest extends AnonymousRequest {

        private final AmqpProducer producer;

        public AnonymousSendCompleteRequest(AnonymousSendRequest open) {
            super(open.getWrappedRequest(), open.envelope);

            this.producer = open.getProducer();
        }

        @Override
        public void onFailure(ProviderException result) {
            LOG.trace("Send phase of anonymous send failed: {} ", getProducerId());
            if (!connection.isAnonymousProducerCache()) {
                AnonymousCloseRequest close = new AnonymousCloseRequest(this);
                producer.close(close);
            }
            super.onFailure(result);
        }

        @Override
        public void onSuccess() {
            LOG.trace("Send phase of anonymous send complete: {} ", getProducerId());
            if (!connection.isAnonymousProducerCache()) {
                AnonymousCloseRequest close = new AnonymousCloseRequest(this);
                producer.close(close);
            } else {
                super.onSuccess();
            }
        }

        @Override
        public AmqpProducer getProducer() {
            return producer;
        }
    }

    private final class AnonymousCloseRequest extends AnonymousRequest {

        private final AmqpProducer producer;

        public AnonymousCloseRequest(AnonymousSendCompleteRequest sendComplete) {
            super(sendComplete.getWrappedRequest(), sendComplete.envelope);

            this.producer = sendComplete.getProducer();
        }

        @Override
        public void onSuccess() {
            LOG.trace("Close phase of anonymous send complete: {} ", getProducerId());
            super.onSuccess();
        }

        @Override
        public AmqpProducer getProducer() {
            return producer;
        }
    }

    private final class CloseRequest implements AsyncResult {

        private final AmqpProducer producer;

        public CloseRequest(AmqpProducer producer) {
            this.producer = producer;
        }

        @Override
        public void onFailure(ProviderException result) {
            AmqpAnonymousFallbackProducer.this.connection.getProvider().fireProviderException(result);
        }

        @Override
        public void onSuccess() {
            LOG.trace("Close of anonymous producer {} complete", producer);
        }

        @Override
        public boolean isComplete() {
            return producer.isClosed();
        }
    }

    private final class AnonymousProducerCache extends LRUCache<JmsDestination, AmqpProducer> {

        private static final long serialVersionUID = 1L;

        public AnonymousProducerCache(int cacheSize) {
            super(cacheSize);
        }

        @Override
        protected void onCacheEviction(Map.Entry<JmsDestination, AmqpProducer> cached) {
            LOG.trace("Producer: {} evicted from producer cache", cached.getValue());
            cached.getValue().close(new CloseRequest(cached.getValue()));
        }
    }
}
