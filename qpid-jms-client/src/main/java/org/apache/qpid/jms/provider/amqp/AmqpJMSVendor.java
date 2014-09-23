/**
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
package org.apache.qpid.jms.provider.amqp;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTemporaryQueue;
import org.apache.qpid.jms.JmsTemporaryTopic;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.message.JmsDefaultMessageFactory;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.proton.jms.JMSVendor;

/**
 * Vendor instance used with Proton-J JMS Transformer bits.
 *
 * TODO - This can go once we have our own message wrappers all working.
 */
public class AmqpJMSVendor extends JMSVendor {

    public static final AmqpJMSVendor INSTANCE = new AmqpJMSVendor();

    private final JmsMessageFactory factory = new JmsDefaultMessageFactory();

    private AmqpJMSVendor() {
    }

    @Override
    public BytesMessage createBytesMessage() {
        try {
            return factory.createBytesMessage();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StreamMessage createStreamMessage() {
        try {
            return factory.createStreamMessage();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Message createMessage() {
        try {
            return factory.createMessage();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        try {
            return factory.createTextMessage();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage() {
        try {
            return factory.createObjectMessage();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MapMessage createMapMessage() {
        try {
            return factory.createMapMessage();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Destination createDestination(String name) {
        return super.createDestination(name, Destination.class);
    }

    @Override
    public <T extends Destination> T createDestination(String name, Class<T> kind) {
        if (kind == Queue.class) {
            return kind.cast(new JmsQueue(name));
        }
        if (kind == Topic.class) {
            return kind.cast(new JmsTopic(name));
        }
        if (kind == TemporaryQueue.class) {
            return kind.cast(new JmsTemporaryQueue(name));
        }
        if (kind == TemporaryTopic.class) {
            return kind.cast(new JmsTemporaryTopic(name));
        }

        return kind.cast(new JmsQueue(name));
    }

    @Override
    public void setJMSXUserID(Message msg, String value) {
        ((JmsMessage) msg).getFacade().setUserId(value);
    }

    @Override
    public void setJMSXGroupID(Message msg, String value) {
        ((JmsMessage) msg).getFacade().setGroupId(value);
    }

    @Override
    public void setJMSXGroupSequence(Message msg, int value) {
        ((JmsMessage) msg).getFacade().setGroupSequence(value);
    }

    @Override
    public void setJMSXDeliveryCount(Message msg, long value) {
        // Delivery count tracks total deliveries which is always one higher than
        // re-delivery count since first delivery counts to.
        ((JmsMessage) msg).getFacade().setRedeliveryCounter((int) (value == 0 ? value : value - 1));
    }

    @Override
    public String toAddress(Destination dest) {
        return ((JmsDestination) dest).getName();
    }
}
