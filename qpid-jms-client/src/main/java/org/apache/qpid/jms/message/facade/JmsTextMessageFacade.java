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
package org.apache.qpid.jms.message.facade;

import javax.jms.JMSException;

/**
 * A Facade around a provider message that behaves like a TextMessage instance.
 */
public interface JmsTextMessageFacade extends JmsMessageFacade {

    /**
     * Creates a copy of this JmsTextMessageFacade and its underlying
     * provider message instance.
     *
     * @return a new JmsTextMessageFacade that wraps a duplicate message.
     */
    @Override
    JmsTextMessageFacade copy() throws JMSException;

    /**
     * Returns the String payload of the Message or null if none set.
     *
     * @return the String value contained in the message.
     *
     * @throws JMSException if an error occurs while decoding text payload.
     */
    String getText() throws JMSException;

    /**
     * Sets the new String payload of the wrapped message, or clears the old value
     * if the given value is null.
     *
     * @param value
     *        the new String payload, or null to clear the contents.
     */
    void setText(String value);

}
