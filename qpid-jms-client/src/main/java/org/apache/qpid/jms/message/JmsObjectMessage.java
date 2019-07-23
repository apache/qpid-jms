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
package org.apache.qpid.jms.message;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;

import org.apache.qpid.jms.message.facade.JmsObjectMessageFacade;

public class JmsObjectMessage extends JmsMessage implements ObjectMessage {

    private final JmsObjectMessageFacade facade;

    public JmsObjectMessage(JmsObjectMessageFacade facade) {
        super(facade);
        this.facade = facade;
    }

    @Override
    public JmsObjectMessage copy() throws JMSException {
        JmsObjectMessage other = new JmsObjectMessage(facade.copy());
        other.copy(this);
        return other;
    }

    @Override
    public void setObject(Serializable newObject) throws JMSException {
        checkReadOnlyBody();
        try {
            this.facade.setObject(newObject);
        } catch (Exception e) {
            MessageFormatException jmsEx = new MessageFormatException("Failed to serialize object:" + e.getMessage());
            jmsEx.setLinkedException(e);
            jmsEx.initCause(e);
            throw jmsEx;
        }
    }

    @Override
    public Serializable getObject() throws JMSException {
        try {
            return this.facade.getObject();
        } catch (Exception e) {
            MessageFormatException jmsEx = new MessageFormatException("Failed to read object: " + e.getMessage());
            jmsEx.setLinkedException(e);
            jmsEx.initCause(e);
            throw jmsEx;
        }
    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        if (!facade.hasBody()) {
            return true;
        }

        return Serializable.class == target || Object.class == target || target.isInstance(getObject());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        try {
            return (T) getObject();
        } catch (JMSException e) {
            MessageFormatException jmsEx = new MessageFormatException("Failed to read object: " + e.getMessage());
            jmsEx.setLinkedException(e);
            jmsEx.initCause(e);
            throw jmsEx;
        }
    }

    @Override
    public String toString() {
        return "JmsObjectMessageFacade { " + facade.toString() + " }";
    }
}
