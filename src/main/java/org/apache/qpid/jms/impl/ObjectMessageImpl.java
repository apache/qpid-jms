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

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.apache.qpid.jms.engine.AmqpObjectMessage;

public class ObjectMessageImpl extends MessageImpl<AmqpObjectMessage> implements ObjectMessage
{
    public ObjectMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        this(new AmqpObjectMessage(), sessionImpl, connectionImpl);
    }

    public ObjectMessageImpl(AmqpObjectMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl);
    }

    @Override
    protected AmqpObjectMessage prepareUnderlyingAmqpMessageForSending(AmqpObjectMessage amqpMessage)
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    //======= JMS Methods =======

    @Override
    public void setObject(Serializable object) throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public Serializable getObject() throws JMSException
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }
}
