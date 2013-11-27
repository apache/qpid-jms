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

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.qpid.jms.engine.AmqpTextMessage;

public class TextMessageImpl extends MessageImpl<AmqpTextMessage> implements TextMessage
{
    //message to be sent
    public TextMessageImpl(SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        this(new AmqpTextMessage(), sessionImpl, connectionImpl);
    }

    //message just received
    public TextMessageImpl(AmqpTextMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl) throws JMSException
    {
        super(amqpMessage, sessionImpl, connectionImpl);
    }

    @Override
    protected AmqpTextMessage prepareUnderlyingAmqpMessageForSending(AmqpTextMessage amqpMessage)
    {
        //Nothing to do here currently, the message operations are all
        //already operating on the AmqpMessage directly

        //TODO: do we need to do anything later with properties/headers etc?
        return amqpMessage;
    }

    //======= JMS Methods =======

    @Override
    public String getText() throws JMSException
    {
        return getUnderlyingAmqpMessage(false).getText();
    }

    @Override
    public void setText(String text) throws JMSException
    {
        //TODO: checkWritable();
        getUnderlyingAmqpMessage(false).setText(text);
    }
}
