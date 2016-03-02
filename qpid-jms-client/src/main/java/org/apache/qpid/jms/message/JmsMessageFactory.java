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

/**
 * Interface that a Provider should implement to provide a Provider
 * Specific JmsMessage implementation that optimizes the exchange of
 * message properties and payload between the JMS Message API and the
 * underlying Provider Message implementations.
 */
public interface JmsMessageFactory {

    /**
     * Creates an instance of a basic JmsMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsMessage instance.
     *
     * @throws JMSException if the provider cannot create the message for some reason.
     */
    JmsMessage createMessage() throws JMSException;

    /**
     * Creates an instance of a basic JmsTextMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @param payload
     *        The value to initially assign to the Message body, or null if empty to start.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     *
     * @throws JMSException if the provider cannot create the message for some reason.
     */
    JmsTextMessage createTextMessage(String payload) throws JMSException;

    /**
     * Creates an instance of a basic JmsTextMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     *
     * @throws JMSException if the provider cannot create the message for some reason.
     */
    JmsTextMessage createTextMessage() throws JMSException;

    /**
     * Creates an instance of a basic JmsBytesMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     *
     * @throws JMSException if the provider cannot create the message for some reason.
     */
    JmsBytesMessage createBytesMessage() throws JMSException;

    /**
     * Creates an instance of a basic JmsMapMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     *
     * @throws JMSException if the provider cannot create the message for some reason.
     */
    JmsMapMessage createMapMessage() throws JMSException;

    /**
     * Creates an instance of a basic JmsStreamMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsTextMessage instance.
     *
     * @throws JMSException if the provider cannot create the message for some reason.
     */
    JmsStreamMessage createStreamMessage() throws JMSException;

    /**
     * Creates an instance of a basic JmsObjectMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @param payload
     *        The value to initially assign to the Message body, or null if empty to start.
     *
     * @return a newly created and initialized JmsObjectMessage instance.
     *
     * @throws JMSException if the provider cannot create the message for some reason.
     */
    JmsObjectMessage createObjectMessage(Serializable payload) throws JMSException;

    /**
     * Creates an instance of a basic JmsObjectMessage object.  The provider may
     * either create the Message with the default generic internal message
     * implementation or create a Provider specific instance that optimizes
     * the access and marshaling of the message.
     *
     * @return a newly created and initialized JmsObjectMessage instance.
     *
     * @throws JMSException if the provider cannot create the message for some reason.
     */
    JmsObjectMessage createObjectMessage() throws JMSException;

}
