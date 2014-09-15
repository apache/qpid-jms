/*
 *
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
 *
 */
package org.apache.qpid.jms.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.impl.ClientProperties;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpListMessageTest extends QpidJmsTestCase
{
    private AmqpConnection _mockAmqpConnection;
    private Delivery _mockDelivery;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
        _mockDelivery = Mockito.mock(Delivery.class);
    }

    @Test
    public void testNewMessageToSendContainsMessageTypeAnnotation() throws Exception
    {
        AmqpMessage amqpListMessage = new AmqpListMessage();
        assertTrue("expected message type annotation to be present", amqpListMessage.messageAnnotationExists(ClientProperties.X_OPT_JMS_MSG_TYPE));
        assertEquals("unexpected value for message type annotation value", ClientProperties.STREAM_MESSAGE_TYPE, amqpListMessage.getMessageAnnotation(ClientProperties.X_OPT_JMS_MSG_TYPE));
    }

    @Test
    public void testGetWithNewMessageToSendThrowsIOOBE() throws Exception
    {
        AmqpListMessage amqpListMessage = new AmqpListMessage();

        checkForIOOBE(amqpListMessage);
    }

    @Test
    public void testGetUsingReceivedMessageReturnsExpectedValue() throws Exception
    {
        Message message = Proton.message();
        List<Object> list = new ArrayList<Object>();
        list.add(Boolean.FALSE);
        message.setBody(new AmqpValue(list));

        AmqpListMessage amqpListMessage = new AmqpListMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals("Unexpected value retrived", Boolean.FALSE, amqpListMessage.get());
    }

    @Test
    public void testResetPosition() throws Exception
    {
        AmqpListMessage amqpListMessage = new AmqpListMessage();

        //add something
        amqpListMessage.add(Boolean.TRUE);

        //check we can access it, before IOOBE
        checkSingleAccessWorksBeforeIOOBE(amqpListMessage, Boolean.TRUE);

        //reset position
        amqpListMessage.resetPosition();

        //check it once more, should work again before another IOOBE
        checkSingleAccessWorksBeforeIOOBE(amqpListMessage, Boolean.TRUE);
    }

    private void checkSingleAccessWorksBeforeIOOBE(AmqpListMessage amqpListMessage, Object expected)
    {
        //check the value is as expected
        assertEquals("unexpected value", expected, amqpListMessage.get());

        //verify getting again results in IOOBE as there is nothing left
        checkForIOOBE(amqpListMessage);
    }

    private void checkForIOOBE(AmqpListMessage amqpListMessage)
    {
        try
        {
            amqpListMessage.get();
            fail("Expected exception to be thrown");
        }
        catch(IndexOutOfBoundsException ioobe)
        {
            //expected
        }
    }

    @Test
    public void testClear() throws Exception
    {
        AmqpListMessage amqpListMessage = new AmqpListMessage();

        //add some stuff
        amqpListMessage.add(Boolean.TRUE);
        amqpListMessage.add(Boolean.FALSE);

        //retrieve only some of it, leaving some unread
        assertEquals("unexpected value", Boolean.TRUE, amqpListMessage.get());

        //clear
        amqpListMessage.clear();

        //add something else
        amqpListMessage.add(Character.valueOf('c'));

        //check we can get it alone before another IOOBE (i.e position was reset, other contents cleared)
        checkSingleAccessWorksBeforeIOOBE(amqpListMessage, Character.valueOf('c'));
    }

    @Test
    public void testCreateAmqpListMessageWithUnexpectedBodySectionTypeThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpSequence(new ArrayList<Object>()));

        try
        {
            new AmqpListMessage(message, _mockDelivery, _mockAmqpConnection);
            fail("expected exception to be thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }

    @Test
    public void testCreateAmqpListMessageWithAmqpValueBodySectionContainingUnexpectedValueThrowsISE() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue("not-a-list"));

        try
        {
            new AmqpListMessage(message, _mockDelivery, _mockAmqpConnection);
            fail("expected exception to be thrown");
        }
        catch(IllegalStateException ise)
        {
            //expected
        }
    }

    @Test
    public void testCreateAmqpListMessageWithEmptyAmqpValueBodySection() throws Exception
    {
        Message message = Proton.message();
        message.setBody(new AmqpValue(null));

        AmqpListMessage amqpListMessage = new AmqpListMessage(message, _mockDelivery, _mockAmqpConnection);

        //Should be able to use the message, e.g clearing it and adding to it.
        amqpListMessage.clear();
        amqpListMessage.add("myString");
    }
}