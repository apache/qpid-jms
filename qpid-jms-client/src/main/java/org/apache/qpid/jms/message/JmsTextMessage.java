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

import jakarta.jms.JMSException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.TextMessage;

import org.apache.qpid.jms.message.facade.JmsTextMessageFacade;

@SuppressWarnings("unchecked")
public class JmsTextMessage extends JmsMessage implements TextMessage {

    private final JmsTextMessageFacade facade;

    public JmsTextMessage(JmsTextMessageFacade facade) {
        super(facade);
        this.facade = facade;
    }

    @Override
    public JmsTextMessage copy() throws JMSException {
        JmsTextMessage other = new JmsTextMessage(facade.copy());
        other.copy(this);
        return other;
    }

    private void copy(JmsTextMessage other) throws JMSException {
        super.copy(other);
    }

    @Override
    public void setText(String text) throws JMSException, MessageNotWriteableException {
        checkReadOnlyBody();
        this.facade.setText(text);
    }

    @Override
    public String getText() throws JMSException {
        return facade.getText();
    }

    @Override
    public String toString() {
        return "JmsTextMessage { " + facade.toString() + " }";
    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        return facade.hasBody() ? target.isAssignableFrom(String.class) : true;
    }

    @Override
    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        return (T) getText();
    }
}
