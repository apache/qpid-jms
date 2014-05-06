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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.apache.qpid.jms.engine.AmqpObjectMessage;
import org.apache.qpid.jms.engine.AmqpSerializedObjectMessage;

//TODO: support requesting to send an AMQP map/list/value instead of serialized binary data
public class ObjectMessageImpl extends MessageImpl<AmqpObjectMessage> implements ObjectMessage
{
    //message to be sent
    public ObjectMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(new AmqpSerializedObjectMessage(), sessionImpl, connectionImpl);
    }

    //message just received
    public ObjectMessageImpl(AmqpObjectMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }

    @Override
    protected AmqpObjectMessage prepareUnderlyingAmqpMessageForSending(AmqpObjectMessage amqpMessage)
    {
        //Currently nothing to do, we [de]serialize the bytes direct to/from the underlying message.
        return amqpMessage;

        //TODO: verify we haven't been requested to send an AMQP map/list instead of serialized binary data,
        //we might need to convert if we have been asked to (or not to).
    }

    //======= JMS Methods =======

    @Override
    public void setObject(Serializable serializable) throws JMSException
    {
        checkBodyWritable();
        try
        {
            getUnderlyingAmqpMessage(false).setObject(serializable);
        }
        catch (Exception e)
        {
            throw new QpidJmsMessageFormatException("Exception while setting Object", e);
        }
    }

    @Override
    public Serializable getObject() throws JMSException
    {
        try
        {
            return getUnderlyingAmqpMessage(false).getObject();
        }
        catch (Exception e)
        {
            throw new QpidJmsMessageFormatException("Exception while getting Object", e);
        }
    }

    @Override
    public void clearBody() throws JMSException
    {
        try
        {
            getUnderlyingAmqpMessage(false).setObject(null);
            setBodyWritable(true);
        }
        catch (Exception e)
        {
            throw new QpidJmsMessageFormatException("Exception while clearing Object body", e);
        }
    }
}
