/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.impl;

import org.apache.qpid.jms.engine.AmqpConnection;
import org.apache.qpid.jms.engine.AmqpSentMessage;
import org.apache.qpid.proton.TimeoutException;

public class SentMessageImpl
{
    private AmqpSentMessage _sentMessage;
    private SenderImpl _sender;

    public SentMessageImpl(AmqpSentMessage sentMessage, SenderImpl sender)
    {
        _sentMessage = sentMessage;
        _sender = sender;
    }

    public void waitUntilAccepted() throws TimeoutException, InterruptedException
    {
        _sender.getConnectionImpl().waitUntil(new SimplePredicate("Remote delivery state exists", _sentMessage)
        {
            @Override
            public boolean test()
            {
                return _sentMessage.getRemoteDeliveryState() != null;
            }
        }, AmqpConnection.TIMEOUT);
    }

    /**
     * {@inheritDoc}
     * <br/>
     * Not thread-safe
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SentMessageImpl [_sentMessage=").append(_sentMessage)
            .append("]");
        return builder.toString();
    }


}
