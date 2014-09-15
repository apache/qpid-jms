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

import static org.apache.qpid.jms.impl.ClientProperties.JMS_AMQP_TYPED_ENCODING;

import java.io.Serializable;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.apache.qpid.jms.engine.AmqpObjectMessage;

public class ObjectMessageImpl extends MessageImpl<AmqpObjectMessage> implements ObjectMessage
{
    //TODO: add a way of controlling this: per connection and client wide?
    private Boolean _defaultUseAmqpTypeEncoding = false;

    //message to be sent
    public ObjectMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(new AmqpObjectMessage(), sessionImpl, connectionImpl);
    }

    //message just received
    public ObjectMessageImpl(AmqpObjectMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }

    @Override
    protected AmqpObjectMessage prepareUnderlyingAmqpMessageForSending(AmqpObjectMessage amqpMessage)
    {
        //Currently nothing to do, we [de]serialize the bytes/bodies direct to/from the underlying message.
        return amqpMessage;
    }

    @Override
    void notifyChangeJMS_AMQP_TYPED_ENCODING(Boolean value) throws QpidJmsMessageFormatException
    {
        /* TODO
         *
         * JMS_AMQP_TYPED_ENCODING as a means of controlling/signalling whether an ObjectMessage is
         * sent/received as serialized Java, or using the AMQP type system.
         *
         * NOTES/Questions:
         *
         * # We need to support converting from one type to the other with existing content, because we can't control when another JMS provider will set the property relative to the content.
         *
         * # If we don't put it in the result of getPropertyNames() then it wont survive a 're-populate the properties' by clearing and setting them again
         *   - happens when being sent by another provider
         *   - being used by an app that wants to remove properties or add properties to a received message even with the same provider
         *
         * # If we do put it in the property names, clearing the property names either has to:
         *   - leave that special property present to keep signalling what will happen when sending the message
         *   - clear the property and if necessary (depends on the default) alter the encoding type of the body (which might not be cleared)
         *   - clear the property but regardless NOT alter the type of the body (which might not be cleared)
         *
         * # Do we add it to the property names if the connection/client has an [overriding] default configuration?
         *
         * # Do we add it to the property names for ObjectMessages which are received with the AMQP type encoding?
         */
        boolean useAmqpTypeEnc =_defaultUseAmqpTypeEncoding;
        if(value != null)
        {
            useAmqpTypeEnc = value;
        }

        try
        {
            getUnderlyingAmqpMessage(false).setUseAmqpTypeEncoding(useAmqpTypeEnc);
        }
        catch (Exception e)
        {
            throw new QpidJmsMessageFormatException("Exception setting " + JMS_AMQP_TYPED_ENCODING, e);
        }
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
