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

import org.apache.qpid.proton.amqp.messaging.AmqpValue;

public class TextMessageImpl extends MessageImpl implements TextMessage
{
    public TextMessageImpl() throws JMSException
    {
        super();
    }

    public TextMessageImpl(String text) throws JMSException
    {
        setText(text);
    }

    @Override
    public String getText() throws JMSException
    {
        // PHTODO Auto-generated method stub
        throw new UnsupportedOperationException("PHTODO");
    }

    @Override
    public void setText(String string) throws JMSException
    {
        AmqpValue body = new AmqpValue(string);

        //TODO: stop accessing the Proton Message directly
        getAmqpMessage().getMessage().setBody(body);
    }

}
