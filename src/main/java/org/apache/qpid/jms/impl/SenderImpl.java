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

import org.apache.qpid.jms.engine.AmqpSender;
import org.apache.qpid.jms.engine.AmqpSentMessage;
import org.apache.qpid.proton.TimeoutException;
import org.apache.qpid.proton.message.Message;

public class SenderImpl extends LinkImpl
{
    private AmqpSender _amqpSender;

    public SenderImpl(SessionImpl sessionImpl, AmqpSender amqpSender)
    {
        super(sessionImpl, amqpSender);
        _amqpSender = amqpSender;
    }

    public void sendMessage(Message message) throws TimeoutException, InterruptedException
    {
        getConnectionImpl().lock();
        try
        {
            AmqpSentMessage sentMessage = _amqpSender.sendMessage(message);

            getConnectionImpl().stateChanged();

            SentMessageImpl sentMessageImpl = new SentMessageImpl(sentMessage, this);
            sentMessageImpl.waitUntilAccepted();
            sentMessage.settle();
        }
        finally
        {
            getConnectionImpl().releaseLock();
        }

    }
}
