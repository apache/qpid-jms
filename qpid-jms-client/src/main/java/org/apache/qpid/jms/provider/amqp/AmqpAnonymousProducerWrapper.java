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

import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.proton.engine.EndpointState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the case of anonymous JMS MessageProducers.
 *
 * In order to simulate the anonymous producer we must create a sender for each message
 * send attempt and close it following a successful send.
 */
public class AmqpAnonymousProducerWrapper extends AmqpProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAnonymousProducerWrapper.class);
    AmqpProducer delegate;

    /**
     * Creates the Anonymous Producer object.
     *
     * @param session
     *        the session that owns this producer
     * @param info
     *        the JmsProducerInfo for this producer.
     */
    public AmqpAnonymousProducerWrapper(AmqpSession session, JmsProducerInfo info) {
        super(session, info);

        delegate = new AmqpFixedProducer(session, info);
    }

    @Override
    public boolean send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        LOG.trace("Delegating anonymous send to underlying producer: {}", getProducerId());

       return delegate.send(envelope, request);
    }

    @Override
    public void open(AsyncResult request) {
        AnonymousRelayRequest anonRelayRequest = new AnonymousRelayRequest(request);
        delegate.open(anonRelayRequest);
    }

    @Override
    public void close(AsyncResult request) {
        delegate.close(request);
    }

    @Override
    public boolean isAnonymous() {
        return true;
    }

    @Override
    public EndpointState getLocalState() {
        return delegate.getLocalState();
    }

    @Override
    public EndpointState getRemoteState() {
        return delegate.getRemoteState();
    }

    @Override
    public void setPresettle(boolean presettle) {
        delegate.setPresettle(presettle);
    };

    private class AnonymousRelayRequest extends WrappedAsyncResult {

        public AnonymousRelayRequest(AsyncResult openResult) {
            super(openResult);
        }

        /**
         * If creation of the producer to the anonymous-relay failed, we try to
         * enter fallback mode rather than immediately failing.
         */
        @Override
        public void onFailure(Throwable result) {
            LOG.debug("Attempt to open producer to anonymous relay failed, entering fallback mode");

            AmqpProducer newProducer = new AmqpAnonymousFallbackProducer(session, getJmsResource());
            newProducer.setPresettle(delegate.isPresettle());
            delegate = newProducer;

            delegate.open(getWrappedRequest());
        }
    }
}
