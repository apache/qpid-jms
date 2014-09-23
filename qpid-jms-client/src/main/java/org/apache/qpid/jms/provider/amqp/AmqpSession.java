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

import java.util.HashMap;
import java.util.Map;

import javax.jms.IllegalStateException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.proton.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpSession extends AbstractAmqpResource<JmsSessionInfo, Session> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSession.class);

    private final AmqpConnection connection;
    private final AmqpTransactionContext txContext;

    private final Map<JmsConsumerId, AmqpConsumer> consumers = new HashMap<JmsConsumerId, AmqpConsumer>();
    private final Map<JmsProducerId, AmqpProducer> producers = new HashMap<JmsProducerId, AmqpProducer>();

    public AmqpSession(AmqpConnection connection, JmsSessionInfo info) {
        super(info, connection.getProtonConnection().session());
        this.connection = connection;

        this.info.getSessionId().setProviderHint(this);
        if (this.info.isTransacted()) {
            txContext = new AmqpTransactionContext(this);
        } else {
            txContext = null;
        }
    }

    @Override
    public void opened() {
        if (this.txContext != null) {
            this.txContext.open(openRequest);
        } else {
            super.opened();
        }
    }

    @Override
    protected void doOpen() {
        this.endpoint.setIncomingCapacity(Integer.MAX_VALUE);
        this.connection.addSession(this);
    }

    @Override
    protected void doClose() {
        this.connection.removeSession(this);
    }

    /**
     * Perform an acknowledge of all delivered messages for all consumers active in this
     * Session.
     */
    public void acknowledge() {
        for (AmqpConsumer consumer : consumers.values()) {
            consumer.acknowledge();
        }
    }

    /**
     * Perform re-send of all delivered but not yet acknowledged messages for all consumers
     * active in this Session.
     *
     * @throws Exception if an error occurs while performing the recover.
     */
    public void recover() throws Exception {
        for (AmqpConsumer consumer : consumers.values()) {
            consumer.recover();
        }
    }

    public AmqpProducer createProducer(JmsProducerInfo producerInfo) {
        AmqpProducer producer = null;

        // TODO - There seems to be an issue with Proton not allowing links with a Target
        //        that has no address.  Otherwise we could just ensure that messages sent
        //        to these anonymous targets have their 'to' value set to the destination.
        if (producerInfo.getDestination() != null) {
            LOG.debug("Creating fixed Producer for: {}", producerInfo.getDestination());
            producer = new AmqpFixedProducer(this, producerInfo);
        } else {
            LOG.debug("Creating an Anonymous Producer: ");
            producer = new AmqpAnonymousProducer(this, producerInfo);
        }

        producer.setPresettle(connection.isPresettleProducers());

        return producer;
    }

    public AmqpProducer getProducer(JmsProducerInfo producerInfo) {
        return getProducer(producerInfo.getProducerId());
    }

    public AmqpProducer getProducer(JmsProducerId producerId) {
        if (producerId.getProviderHint() instanceof AmqpProducer) {
            return (AmqpProducer) producerId.getProviderHint();
        }
        return this.producers.get(producerId);
    }

    public AmqpConsumer createConsumer(JmsConsumerInfo consumerInfo) {
        AmqpConsumer result = null;

        if (consumerInfo.isBrowser()) {
            result = new AmqpQueueBrowser(this, consumerInfo);
        } else {
            result = new AmqpConsumer(this, consumerInfo);
        }

        result.setPresettle(connection.isPresettleConsumers());
        return result;
    }

    public AmqpConsumer getConsumer(JmsConsumerInfo consumerInfo) {
        return getConsumer(consumerInfo.getConsumerId());
    }

    public AmqpConsumer getConsumer(JmsConsumerId consumerId) {
        if (consumerId.getProviderHint() instanceof AmqpConsumer) {
            return (AmqpConsumer) consumerId.getProviderHint();
        }
        return this.consumers.get(consumerId);
    }

    public AmqpTransactionContext getTransactionContext() {
        return this.txContext;
    }

    /**
     * Begins a new Transaction using the given Transaction Id as the identifier.  The AMQP
     * binary Transaction Id will be stored in the provider hint value of the given transaction.
     *
     * @param txId
     *        The JMS Framework's assigned Transaction Id for the new TX.
     * @param request
     *        The request that will be signaled on completion of this operation.
     *
     * @throws Exception if an error occurs while performing the operation.
     */
    public void begin(JmsTransactionId txId, AsyncResult request) throws Exception {
        if (!this.info.isTransacted()) {
            throw new IllegalStateException("Non-transacted Session cannot start a TX.");
        }

        getTransactionContext().begin(txId, request);
    }

    /**
     * Commit the currently running Transaction.
     *
     * @param request
     *        The request that will be signaled on completion of this operation.
     *
     * @throws Exception if an error occurs while performing the operation.
     */
    public void commit(AsyncResult request) throws Exception {
        if (!this.info.isTransacted()) {
            throw new IllegalStateException("Non-transacted Session cannot start a TX.");
        }

        getTransactionContext().commit(request);
    }

    /**
     * Roll back the currently running Transaction
     *
     * @param request
     *        The request that will be signaled on completion of this operation.
     *
     * @throws Exception if an error occurs while performing the operation.
     */
    public void rollback(AsyncResult request) throws Exception {
        if (!this.info.isTransacted()) {
            throw new IllegalStateException("Non-transacted Session cannot start a TX.");
        }

        getTransactionContext().rollback(request);
    }

    void addResource(AmqpConsumer consumer) {
        consumers.put(consumer.getConsumerId(), consumer);
    }

    void removeResource(AmqpConsumer consumer) {
        consumers.remove(consumer.getConsumerId());
    }

    void addResource(AmqpProducer producer) {
        producers.put(producer.getProducerId(), producer);
    }

    void removeResource(AmqpProducer producer) {
        producers.remove(producer.getProducerId());
    }

    /**
     * Adds Topic or Queue qualifiers to the destination target.  We don't add qualifiers to
     * Temporary Topics and Queues since AMQP works a bit differently.
     *
     * @param destination
     *        The destination to Qualify.
     *
     * @return the qualified destination name.
     */
    public String getQualifiedName(JmsDestination destination) {
        if (destination == null) {
            return null;
        }

        String result = destination.getName();

        if (!destination.isTemporary()) {
            if (destination.isTopic()) {
                result = connection.getTopicPrefix() + destination.getName();
            } else {
                result = connection.getQueuePrefix() + destination.getName();
            }
        }

        return result;
    }

    public AmqpProvider getProvider() {
        return this.connection.getProvider();
    }

    public AmqpConnection getConnection() {
        return this.connection;
    }

    public JmsSessionId getSessionId() {
        return this.info.getSessionId();
    }

    public Session getProtonSession() {
        return this.endpoint;
    }

    boolean isTransacted() {
        return this.info.isTransacted();
    }

    boolean isAsyncAck() {
        return this.info.isSendAcksAsync() || isTransacted();
    }

    @Override
    public String toString() {
        return "AmqpSession { " + getSessionId() + " }";
    }
}
