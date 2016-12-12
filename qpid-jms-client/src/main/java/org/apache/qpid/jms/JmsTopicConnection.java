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
package org.apache.qpid.jms;

import javax.jms.ConnectionConsumer;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.provider.Provider;

public class JmsTopicConnection extends JmsConnection implements AutoCloseable {

    public JmsTopicConnection(JmsConnectionInfo connectionInfo, Provider provider) throws JMSException {
        super(connectionInfo, provider);
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new javax.jms.IllegalStateException("Operation not supported by a TopicConnection");
    }

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        throw new javax.jms.IllegalStateException("Operation not supported by a TopicConnection");
    }
}
