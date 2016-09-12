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
package org.apache.qpid.jms;

import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;

@SuppressWarnings("unused")
public class JmsConsumer implements JMSConsumer, AutoCloseable {

    private final JmsSession session;
    private final JmsMessageConsumer consumer;

    public JmsConsumer(JmsSession session, JmsMessageConsumer consumer) {
        this.session = session;
        this.consumer = consumer;
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    //----- MessageConsumer Property Methods ---------------------------------//

    @Override
    public MessageListener getMessageListener() {
        try {
            return consumer.getMessageListener();
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public String getMessageSelector() {
        try {
            return consumer.getMessageSelector();
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public void setMessageListener(MessageListener listener) {
        try {
            consumer.setMessageListener(listener);
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    //----- Receive Methods --------------------------------------------------//

    @Override
    public Message receive() {
        try {
            return consumer.receive();
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public Message receive(long timeout) {
        try {
            return consumer.receive(timeout);
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public Message receiveNoWait() {
        try {
            return consumer.receiveNoWait();
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> desired) {
        try {
            return consumer.receiveBody(desired, -1);
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> desired, long timeout) {
        try {
            // Configure for infinite wait when timeout is zero (JMS Spec)
            if (timeout == 0) {
                timeout = -1;
            }

            return consumer.receiveBody(desired, timeout);
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> desired) {
        try {
            return consumer.receiveBody(desired, 0);
        } catch (JMSException e) {
            throw JmsExceptionSupport.createRuntimeException(e);
        }
    }
}
