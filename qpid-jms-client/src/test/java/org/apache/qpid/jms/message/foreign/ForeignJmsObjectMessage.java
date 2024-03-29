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
package org.apache.qpid.jms.message.foreign;

import java.io.Serializable;

import jakarta.jms.JMSException;
import jakarta.jms.ObjectMessage;

import org.apache.qpid.jms.message.JmsObjectMessage;
import org.apache.qpid.jms.message.facade.test.JmsTestObjectMessageFacade;

/**
 * Foreign JMS ObjectMessage class
 */
public class ForeignJmsObjectMessage extends ForeignJmsMessage implements ObjectMessage {

    private final JmsObjectMessage message;

    public ForeignJmsObjectMessage() {
        super(new JmsObjectMessage(new JmsTestObjectMessageFacade()));
        this.message = (JmsObjectMessage) super.message;
    }

    @Override
    public void setObject(Serializable object) throws JMSException {
        message.setObject(object);
    }

    @Override
    public Serializable getObject() throws JMSException {
        return message.getObject();
    }
}
