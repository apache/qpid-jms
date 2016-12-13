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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageEOFException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Tests that messages sent and received don't lose data and have expected
 * JMS Message property values.
 */
public class JmsMessageIntegrityTest extends AmqpTestSupport {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connection = createAmqpConnection();
    }

    @Test
    public void testTextMessage() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        {
            TextMessage message = session.createTextMessage();
            message.setText("Hi");
            producer.send(message);
        }
        {
            TextMessage message = (TextMessage)consumer.receive(3000);
            assertNotNull(message);
            assertEquals("Hi", message.getText());
        }

        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testBytesMessageLength() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        {
            BytesMessage message = session.createBytesMessage();
            message.writeInt(1);
            message.writeInt(2);
            message.writeInt(3);
            message.writeInt(4);
            producer.send(message);
        }
        {
            BytesMessage message = (BytesMessage)consumer.receive(3000);
            assertNotNull(message);
            assertEquals(16, message.getBodyLength());
        }

        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testObjectMessage() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        UUID payload = UUID.randomUUID();

        {
            ObjectMessage message = session.createObjectMessage();
            message.setObject(payload);
            producer.send(message);
        }
        {
            ObjectMessage message = (ObjectMessage)consumer.receive(3000);
            assertNotNull(message);
            assertEquals(payload, message.getObject());
        }
        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testBytesMessage() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        {
            BytesMessage message = session.createBytesMessage();
            message.writeBoolean(true);
            producer.send(message);
        }
        {
            BytesMessage message = (BytesMessage)consumer.receive(3000);
            assertNotNull(message);
            assertTrue(message.readBoolean());

            try {
                message.readByte();
                fail("Expected exception not thrown.");
            } catch (MessageEOFException e) {
            }
        }
        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testStreamMessage() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        {
            StreamMessage message = session.createStreamMessage();
            message.writeString("This is a test to see how it works.");
            producer.send(message);
        }
        {
            StreamMessage message = (StreamMessage)consumer.receive(3000);
            assertNotNull(message);

            // Invalid conversion should throw exception and not move the stream position.
            try {
                message.readByte();
                fail("Should have received NumberFormatException");
            } catch (NumberFormatException e) {
            }

            assertEquals("This is a test to see how it works.", message.readString());

            // Invalid conversion should throw exception and not move the stream position.
            try {
                message.readByte();
                fail("Should have received MessageEOFException");
            } catch (MessageEOFException e) {
            }
        }
        assertNull(consumer.receiveNoWait());
    }

    @Test
    public void testMapMessage() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);

        {
            MapMessage message = session.createMapMessage();
            message.setBoolean("boolKey", true);
            producer.send(message);
        }
        {
            MapMessage message = (MapMessage)consumer.receive(3000);
            assertNotNull(message);
            assertTrue(message.getBoolean("boolKey"));
        }
        assertNull(consumer.receiveNoWait());
    }
}
