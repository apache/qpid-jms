/*
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
 */
package org.apache.qpid.jms.impl;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.qpid.jms.engine.AmqpGenericMessage;

public class GenericAmqpMessageImpl extends MessageImpl<AmqpGenericMessage> implements Message
{
    //message to be sent
    public GenericAmqpMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(new AmqpGenericMessage(), sessionImpl, connectionImpl);
    }

    //message just received
    public GenericAmqpMessageImpl(AmqpGenericMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }

    @Override
    protected AmqpGenericMessage prepareUnderlyingAmqpMessageForSending(AmqpGenericMessage amqpMessage)
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

}
