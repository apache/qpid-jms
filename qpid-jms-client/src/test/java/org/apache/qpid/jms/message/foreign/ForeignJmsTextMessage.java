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

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.message.facade.test.JmsTestTextMessageFacade;

/**
 * Foreign JMS TextMessage class
 */
public class ForeignJmsTextMessage extends ForeignJmsMessage implements TextMessage {

    private final JmsTextMessage message;

    public ForeignJmsTextMessage() {
        super(new JmsTextMessage(new JmsTestTextMessageFacade()));
        this.message = (JmsTextMessage) super.message;
    }

    @Override
    public void setText(String string) throws JMSException {
        message.setText(string);
    }

    @Override
    public String getText() throws JMSException {
        return message.getText();
    }
}
