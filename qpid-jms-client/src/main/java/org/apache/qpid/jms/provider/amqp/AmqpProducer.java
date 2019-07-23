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

import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.proton.engine.Sender;

/**
 * Base class for Producer instances.
 */
public abstract class AmqpProducer extends AmqpAbstractResource<JmsProducerInfo, Sender> {

    protected final AmqpSession session;
    protected final AmqpConnection connection;
    protected boolean presettle;
    protected boolean delayedDeliverySupported;

    public AmqpProducer(AmqpSession session, JmsProducerInfo info) {
        this(session, info, null);
    }

    public AmqpProducer(AmqpSession session, JmsProducerInfo info, Sender endpoint) {
        super(info, endpoint, session);

        this.session = session;
        this.connection = session.getConnection();
    }

    /**
     * Sends the given message
     *
     * @param envelope
     *        The envelope that contains the message and it's targeted destination.
     * @param request
     *        The AsyncRequest that will be notified on send success or failure.
     *
     * @throws ProviderException if an error occurs sending the message
     */
    public abstract void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException;

    /**
     * @return true if this is an anonymous producer or false if fixed to a given destination.
     */
    public abstract boolean isAnonymous();

    /**
     * @return the JmsProducerId that was assigned to this AmqpProducer.
     */
    public JmsProducerId getProducerId() {
        return getResourceInfo().getId();
    }

    /**
     * @return true if the producer should presettle all sent messages.
     */
    public boolean isPresettle() {
        return presettle;
    }

    /**
     * Sets whether the producer will presettle all messages that it sends.  Sending
     * presettled reduces the time it takes to send a message but increases the change
     * of message loss should the connection drop during send.
     *
     * @param presettle
     *        true if all messages are sent settled.
     */
    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    public void setDelayedDeliverySupported(boolean delayedDeliverySupported) {
        this.delayedDeliverySupported = delayedDeliverySupported;
    }
}
