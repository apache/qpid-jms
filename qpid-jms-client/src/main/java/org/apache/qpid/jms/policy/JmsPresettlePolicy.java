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
package org.apache.qpid.jms.policy;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsSession;

/**
 * Interface for building policy objects that control when a MessageProducer or
 * MessageConsumer instance will operate in presettled mode.
 */
public interface JmsPresettlePolicy {

    JmsPresettlePolicy copy();

    /**
     * Determines when a producer will send message presettled.
     * <p>
     * Called when the a producer is being created to determine whether the producer will
     * be configured to send all its message as presettled or not.
     * <p>
     * For an anonymous producer this method is called on each send to allow the policy to
     * be applied to the target destination that the message will be sent to.
     *
     * @param session
     *      the session that owns the producer.
     * @param destination
     *      the destination that the producer will be sending to.
     *
     * @return true if the producer should send presettled.
     */
    boolean isProducerPresttled(JmsSession session, JmsDestination destination);

    /**
     * Determines when a consumer will be created with the settlement mode set to presettled.
     * <p>
     * Called when the a consumer is being created to determine whether the consumer will
     * be configured to request that the remote sends it message that are presettled.
     * <p>
     *
     * @param session
     *      the session that owns the consumer being created.
     * @param destination
     *      the destination that the consumer will be listening to.
     *
     * @return true if the producer should send presettled.
     */
    boolean isConsumerPresttled(JmsSession session, JmsDestination destination);

}