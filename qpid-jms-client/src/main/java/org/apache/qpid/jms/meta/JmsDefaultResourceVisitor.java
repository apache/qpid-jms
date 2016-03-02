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
package org.apache.qpid.jms.meta;

import org.apache.qpid.jms.JmsTemporaryDestination;

/**
 * Default Visitor implementation that does nothing in each method to
 * make custom implementations simpler by only needing to override the
 * visitation cases they need to handle.
 */
public class JmsDefaultResourceVisitor implements JmsResourceVistor{

    @Override
    public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
    }

    @Override
    public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
    }

    @Override
    public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
    }

    @Override
    public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
    }

    @Override
    public void processDestination(JmsTemporaryDestination destination) throws Exception {
    }

    @Override
    public void processTransactionInfo(JmsTransactionInfo transactionInfo) throws Exception {
    }
}
