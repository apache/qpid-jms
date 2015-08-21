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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.jms.IllegalStateException;

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

public class AmqpSession extends AmqpAbstractResource<JmsSessionInfo, Session> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSession.class);

    private final AmqpConnection connection;
    private final AmqpTransactionContext txContext;

    private final Map<JmsConsumerId, AmqpConsumer> consumers = new HashMap<JmsConsumerId, AmqpConsumer>();

    public AmqpSession(AmqpConnection connection, JmsSessionInfo info) {
        super(info, connection.getProtonConnection().session());
        this.connection = connection;

        this.resource.getSessionId().setProviderHint(this);
        if (this.resource.isTransacted()) {
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
        long outgoingWindow = getProvider().getSessionOutgoingWindow();

        Session session = this.getEndpoint();
        session.setIncomingCapacity(Integer.MAX_VALUE);
        if(outgoingWindow >= 0) {
            session.setOutgoingWindow(outgoingWindow);
        }

        this.connection.addSession(this);

        super.doOpen();
    }

    @Override
    protected void doClose() {
        this.connection.removeSession(this);
        super.doClose();
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

        if (producerInfo.getDestination() != null || connection.getProperties().isAnonymousRelaySupported()) {
            LOG.debug("Creating AmqpFixedProducer for: {}", producerInfo.getDestination());
            producer = new AmqpFixedProducer(this, producerInfo);
        } else {
            LOG.debug("Creating an AmqpAnonymousFallbackProducer");
            producer = new AmqpAnonymousFallbackProducer(this, producerInfo);
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

        return null;
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
        if (!this.resource.isTransacted()) {
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
        if (!this.resource.isTransacted()) {
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
        if (!this.resource.isTransacted()) {
            throw new IllegalStateException("Non-transacted Session cannot start a TX.");
        }

        getTransactionContext().rollback(request);
    }

    /**
     * Allows a session resource to schedule a task for future execution.
     *
     * @param task
     *      The Runnable task to be executed after the given delay.
     * @param delay
     *      The delay in milliseconds to schedule the given task for execution.
     *
     * @return a ScheduledFuture instance that can be used to cancel the task.
     */
    public ScheduledFuture<?> schedule(final Runnable task, long delay) {
        if (task == null) {
            LOG.trace("Resource attempted to schedule a null task.");
            return null;
        }

        return getProvider().getScheduler().schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    void addResource(AmqpConsumer consumer) {
        consumers.put(consumer.getConsumerId(), consumer);
    }

    void removeResource(AmqpConsumer consumer) {
        consumers.remove(consumer.getConsumerId());
    }

    /**
     * Query the Session to see if there are any registered consumer instances that have
     * a durable subscription with the given subscription name.
     *
     * @param subscriptionName
     *        the name of the subscription being searched for.
     *
     * @return true if there is a consumer that has the given subscription.
     */
    public boolean containsSubscription(String subscriptionName) {
        for (AmqpConsumer consumer : consumers.values()) {
            if (subscriptionName.equals(consumer.getJmsResource().getSubscriptionName())) {
                return true;
            }
        }

        return false;
    }

    //TODO: unused?
    /**
     * Call to send an error that occurs outside of the normal asynchronous processing
     * of a session resource such as a remote close etc.
     *
     * @param error
     *        The error to forward on to the Provider error event handler.
     */
    public void reportError(Exception error) {
        getConnection().getProvider().fireProviderException(error);
    }

    public AmqpProvider getProvider() {
        return this.connection.getProvider();
    }

    public AmqpConnection getConnection() {
        return this.connection;
    }

    public JmsSessionId getSessionId() {
        return this.resource.getSessionId();
    }

    public Session getProtonSession() {
        return this.getEndpoint();
    }

    boolean isTransacted() {
        return this.resource.isTransacted();
    }

    boolean isAsyncAck() {
        return this.resource.isSendAcksAsync() || isTransacted();
    }

    @Override
    public String toString() {
        return "AmqpSession { " + getSessionId() + " }";
    }
}
